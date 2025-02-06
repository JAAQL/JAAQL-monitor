import traceback
import tempfile

from json import JSONDecodeError

from monitor.version import print_version, VERSION
import sys
import requests
from sys import exit
from getpass import getpass
from inspect import getframeinfo, stack
from datetime import datetime
from base64 import urlsafe_b64encode as b64e, urlsafe_b64decode as b64d
import os
from os.path import dirname
import shutil
import json
import platform
import subprocess

HEADER__security_bypass = "Authentication-Token-Bypass"
HEADER__security_bypass_jaaql = "Authentication-Token-Bypass-Jaaql"
HEADER__security = "Authentication-Token"
HEADER__security_specify_user = "Authentication-Token-Bypass-With-User"
MARKER__bypass = "bypass "
MARKER__jaaql_bypass = "jaaql_bypass "

ENDPOINT__prepare = "/prepare"
ENDPOINT__cron = "/cron"
ENDPOINT__oauth = "/oauth/token"
ENDPOINT__submit = "/submit"
ENDPOINT__attach = "/accounts"
ENDPOINT__attach_batch = "/accounts/batch"
ENDPOINT__dispatchers = "/internal/dispatchers"
ENDPOINT__wipe = "/internal/clean"
ENDPOINT__set_web_config = "/internal/set-web-config"
ENDPOINT__freeze = "/internal/freeze"
ENDPOINT__defrost = "/internal/defrost"

COMMAND__initialiser = "\\"
COMMAND__reset_short = "\\r"
COMMAND__import = "\\import"
COMMAND__reset = "\\reset"
COMMAND__go_short = "\\g"
COMMAND__go = "\\go"
COMMAND__print_short = "\\p"
COMMAND__print = "\\print"
COMMAND__cron = "\\cron"
COMMAND__wipe_dbms = "\\wipe dbms"
COMMAND__switch_jaaql_account_to = "\\switch jaaql account to "
COMMAND__connect_to_database = "\\connect to database "
COMMAND__register_jaaql_account_with = "\\register jaaql account with "
COMMAND__federate_jaaql_account_with = "\\federate jaaql account with @user "
COMMAND__psql = "\\psql "
COMMAND__clone_jaaql_account = "\\clone jaaql account "
COMMAND__attach_email_account = "\\attach email account "
COMMAND__quit_short = "\\q"
COMMAND__quit = "\\quit"
COMMAND__freeze_instance = "\\freeze instance"
COMMAND__defrost_instance = "\\defrost instance"
COMMAND__set_web_config = "\\set web config"
COMMAND__with_parameters = "WITH PARAMETERS {"
COMMAND__with_user = "WITH USER"
COMMAND__and_user = " AND USER "

CONNECT_FOR_CREATEDB = " for createdb"
CONNECT_FOR_EXTENSION_CONFIGURATION = " for extension configuration"

DEFAULT_CONNECTION = "default"
DEFAULT_EMAIL_ACCOUNT = "default"

LINE_LENGTH_MAX = 115
ROWS_MAX = 25

METHOD__post = "POST"
METHOD__get = "GET"

ARGS__encoded_config = ['--encoded-config']
ARGS__config = ['-c', '--config']
ARGS__folder_config = ['-f', '--folder-config']
ARGS__input_file = ['-i', '--input-file']
ARGS__psql_file = ['--psql-input-file']
ARGS__parameter = ['-p', '--parameter']
ARGS__single_query = ['-s', '--single-query']
ARGS__skip_auth = ['-a', '--skip-auth']
ARGS__environment = ['-e', '--environment-file']
ARGS__allow_unused_parameters = ['-a', '--allow-unused-parameters']
ARGS__clone_as_attach = ['--clone-as-attach']
ARGS__slurp_in_location = ['--jaaql-slurp-in-location']
ARGS__cost_only = ['--cost-only']


class JAAQLMonitorException(Exception):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class EOFMarker:
    pass


class ConnectionInfo:
    def __init__(self, host, username, password, database, override_url=None):
        self.host = host
        self.username = username
        self.password = password
        self.database = database
        self.oauth_token = None
        self.override_url = override_url

    def to_dict(self):
        return {
            'host': self.host,
            'username': self.username,
            'password': self.password,
            'database': self.database,
            'oauth_token': self.oauth_token,
            'override_url': self.override_url
        }

    def get_port(self):
        return int(self.host.split(":")[1])

    def get_host(self):
        return self.host.split(":")[0]

    def get_http_url(self):
        if self.override_url is not None:
            return self.override_url

        formatted = self.host
        if not formatted.startswith("http"):
            url_input = "https://www." + formatted
            if not url_input.endswith("/api"):
                url_input += "/api"
        if formatted.startswith("http") and ":6060" not in formatted and not formatted.endswith("/api"):
            formatted += "/api"
        return formatted


FUTURE_TYPE_none = 0
FUTURE_TYPE_input = 1
FUTURE_TYPE_psql = 2


class State:
    def __init__(self):
        self.was_go = False
        self.fetched_query = ""
        self.fetched_stdin = None
        self.connections = {}
        self.connection_info = {}
        self._current_connection = None
        self.file_stack = []
        self.fetched_database = None
        self.is_verbose = False
        self.skip_auth = False
        self.single_query = False
        self.is_debugging = False
        self.file_name = None
        self.cur_file_line = 0
        self.file_lines = []
        self.override_url = None
        self.parameters = {}
        self.query_parameters = None
        self.reading_parameters = False
        self.prevent_unused_parameters = True
        self.clone_as_attach = False
        self.slurp_in_location = None

        self.do_exit = True

        self.future_files = []

        self.database_override = None
        self.is_transactional = True

    def set_current_connection(self, connection, name=DEFAULT_CONNECTION):
        self._current_connection = name
        if connection not in self.connection_info:
            self.connection_info[name] = connection
        self.database_override = None
        self.is_transactional = True

    def is_script(self):
        return self.file_name is not None

    def get_next(self):
        if len(self.future_files) == 0:
            return FUTURE_TYPE_none
        else:
            return self.future_files.pop(0)

    def get_current_connection(self) -> ConnectionInfo:
        if self._current_connection is None:
            if DEFAULT_CONNECTION in self.connections:
                self._current_connection = DEFAULT_CONNECTION
            else:
                print_error(self, "There is no selected connection. Please supply a default connection or switch to a connection first")

        return self.connection_info[self._current_connection]

    def log(self, msg):
        if self.is_verbose:
            print(str(msg))

    def _fetch_oauth_token_for_current_connection(self):
        conn = self.get_current_connection()

        if self.skip_auth:
            conn.oauth_token = {
                HEADER__security_bypass: os.environ.get("JAAQL__SUPER_BYPASS_KEY", "00000-00000"),
                HEADER__security_specify_user: conn.username
            }
        else:
            try:
                oauth_res = requests.post(conn.get_http_url() + ENDPOINT__oauth, json={
                    "username": conn.username,
                    "password": conn.password
                })

                if oauth_res.status_code != 200:
                    print_error(self, "Invalid credentials: response code " + str(oauth_res.status_code) + " content: " + oauth_res.text +
                                " for username '" + conn.username + "'")
                    return None

                conn.oauth_token = {HEADER__security: oauth_res.json()}
            except requests.exceptions.RequestException:
                print_error(self, "Could not connect to JAAQL running on " + conn.host + "\nPlease make sure that JAAQL is running and accessible")

    @staticmethod
    def time_delta_ms(start_time: datetime, end_time: datetime) -> int:
        return int(round((end_time - start_time).total_seconds() * 1000))

    def request_handler(self, method, endpoint, send_json=None, handle_error: bool = True, format_as_query_output: bool = True,
                        compress_output_unless: list = None, line_offset: int = 0):
        conn = self.get_current_connection()
        if conn.oauth_token is None:
            if conn.password.startswith(MARKER__bypass):
                conn.oauth_token = {HEADER__security_bypass: conn.password.split(MARKER__bypass)[1]}
            elif conn.password.startswith(MARKER__jaaql_bypass):
                conn.oauth_token = {HEADER__security_bypass_jaaql: conn.password.split(MARKER__jaaql_bypass)[1]}
            else:
                self._fetch_oauth_token_for_current_connection()

        start_time = datetime.now()
        res = requests.request(method, conn.get_http_url() + endpoint, json=send_json, headers=conn.oauth_token)

        if res.status_code == 401:
            self.log("Refreshing oauth token")
            self._fetch_oauth_token_for_current_connection()
            start_time = datetime.now()
            res = requests.request(method, conn.get_http_url() + endpoint, json=send_json, headers=conn.oauth_token)

        self.log("Request took " + str(State.time_delta_ms(start_time, datetime.now())) + "ms")

        if res.status_code == 200 and format_as_query_output:
            was_explain = False
            if send_json is not None:
                if "query" in send_json:
                    was_explain = len([line for line in split_by_lines(send_json["query"]) if line.strip().startswith("EXPLAIN ANALYZE")]) != 0

            if was_explain:
                rows = res.json()["rows"]
                self.log("")
                self.log("")
                for row in rows:
                    self.log(row[0])
            else:
                format_query_output(self, res.json())
        elif res.status_code == 200:
            if compress_output_unless is not None and isinstance(res.json(), list):
                perhaps_expanded = ["\n    ".join(json.dumps(itm, indent=4 if any([itm.get(val) is not None for val in compress_output_unless]) else None).split("\n")) for itm in res.json()]
                print("[\n    " + ",\n    ".join(perhaps_expanded) + "\n]")
            else:
                print(json.dumps(res.json(), indent=4))
        else:
            if handle_error:
                if endpoint == ENDPOINT__submit:
                    submit_error(self, res.text, line_offset=line_offset)
                else:
                    print_error(self, res.text)

        return res


def split_by_lines(split_str, gap=1):
    split_str = split_str.split("".join(["\r\n"] * gap))
    if len(split_str) == 1:
        split_str = split_str[0].split("".join(["\n"] * gap))
    return [s for s in split_str if len(s.strip()) != 0]


def get_connection_info(state: State, connection_name: str = None, file_name: str = None, override_username: str = None):
    if connection_name == DEFAULT_CONNECTION and DEFAULT_CONNECTION not in state.connections:
        lookup_name = list(state.connections.keys())[0]
        connection_name = lookup_name
    if connection_name and connection_name in state.connection_info:
        return state.connection_info[connection_name]
    elif connection_name and connection_name in state.connections:
        file_name = state.connections[connection_name]

    if file_name is None and connection_name is None:
        print_error(state, "Error in the python script. A connection is being fetched without a name or file")
    elif file_name is None:
        print_error(state, "No named connection: '" + connection_name + "'")

    try:
        config = open(file_name, "r").read()
        config = split_by_lines(config)
        host = config[0].strip()
        username = config[1].strip()
        if override_username is not None:
            username = override_username
        password = config[2].strip()
        database = None
        if len(config) > 3:
            database = config[3].strip()
            if len(database) == 0:
                database = None
            else:
                state.log("Found database '" + database + "'")

        state.log("Successfully loaded config")

        ci = ConnectionInfo(host, username, password, database, state.override_url)

        if connection_name:
            state.connection_info[connection_name] = ci

        return ci
    except FileNotFoundError:
        if connection_name is None:
            print_error(state, "Could not find credentials file located at '" + file_name + "', using working directory " + os.getcwd())
        else:
            print_error(state, "Could not find named credentials file '" + connection_name + "' located at '" + file_name +
                        "', using working directory " + os.getcwd())
    except Exception:
        traceback.print_exc()
        print_error(state, "Could not load the credential file '" + connection_name + "'. Is the file formatted correctly?")


def format_output_row(data, max_length, data_types, breaches):
    builder = ""
    for col, the_length, data_type, did_breach in zip(data, max_length, data_types, breaches):
        col_str = str(col) if col is not None else "null"
        builder += "|"
        spacing = "".join([" "] * max(the_length - len(col_str), 0))
        if did_breach and len(col_str) > the_length:
            col_str = col_str[0:min(the_length, len(col_str)) - 3]
            col_str += "..."
        else:
            col_str = col_str[0:min(the_length, len(col_str))]
        if data_type == str:
            builder += col_str + spacing
        else:
            builder += spacing + col_str
    builder += "|"
    return builder


def format_output_divider(max_length):
    builder = ""

    for x in max_length:
        builder += "+"
        builder += "".join(["-"] * x)

    builder += "+"
    return builder


def format_query_output(state, json_output):
    if "rows" not in json_output:
        return None
    str_num_rows = "(" + str(len(json_output["rows"])) + " " + ("row" if len(json_output["rows"]) == 1 else "rows") + ")"

    if len(json_output["rows"]) > 50:
        state.log(str_num_rows)

    max_length = []
    types = []
    first_pass = True
    for row in json_output["rows"]:
        for col, col_idx in zip(row, range(len(row))):
            col_str = str(col)
            if first_pass:
                max_length.append(len(col_str))
                types.append(type(col))
            elif len(col_str) > max_length[col_idx]:
                max_length[col_idx] = len(col_str)
        first_pass = False

    breaches = [False] * len(max_length)

    while sum(max_length) + len(max_length) > LINE_LENGTH_MAX:
        max_idx = 0
        max_len = 0
        for cur_len, col_idx in zip(max_length, range(len(max_length))):
            if cur_len > max_len:
                max_len = cur_len
                max_idx = col_idx
        breaches[max_idx] = True
        max_length[max_idx] -= 1

    if first_pass:
        for col in json_output["columns"]:
            max_length.append(len(col))

    state.log(format_output_divider(max_length))
    state.log(format_output_row(json_output["columns"], max_length, [str] * len(json_output["columns"]), [False] * len(max_length)))
    state.log(format_output_divider(max_length))

    if len(json_output["rows"]) > ROWS_MAX and not state.file_name:
        json_output["rows"] = json_output["rows"][0:ROWS_MAX]
        json_output["rows"].append(["..." for _ in json_output["columns"]])

    for row in json_output["rows"]:
        state.log(format_output_row(row, max_length, types, breaches))

    if len(json_output["rows"]) != 0:
        state.log(format_output_divider(max_length))

    state.log(str_num_rows)


def handle_login(state, jaaql_url: str = None):
    load_file = False
    username = None
    password = None
    if not jaaql_url:
        jaaql_url = input("Jaaql Url: ")
    elif jaaql_url.startswith("file "):
        return get_connection_info(state, file_name=jaaql_url.split("file ")[1])

    if not load_file:
        username = input("Username: ").strip()
        password = getpass(prompt='Password: ', stream=None)

    return ConnectionInfo(jaaql_url, username, password, None, state.override_url)


def dump_buffer(state, start: str = "\n\n"):
    return ("%sBuffer [" % start) + str(len(state.fetched_query.strip())) + "]:\n" + state.fetched_query.strip() + "\n\n"


def get_message(state, err, line_offset, buffer, additional_line_message: str = ""):
    caller = getframeinfo(stack()[1][0])
    file_message = ""
    if state.file_name is not None:
        file_message = "Using JAAQL " + str(VERSION) + "\nError on " + additional_line_message + "line %d of file '%s':\n" % (state.cur_file_line - line_offset, state.file_name)
    debug_message = " [%s:%d]" % (caller.filename, caller.lineno)
    if not state.is_script() or not state.is_debugging:
        debug_message = ""
    buffer = "\n" + buffer
    if not state.is_script():
        buffer = ""

    try:
        json_err = json.loads(err)
        if json_err.get("message") is not None:
            err = json_err["message"] + "\n\n" + err
    except JSONDecodeError:
        pass

    print(file_message + err + debug_message + buffer, file=sys.stderr)
    if state.is_script():
        if state.do_exit:
            exit(1)
        else:
            raise JAAQLMonitorException(file_message + err + debug_message + buffer)


def submit_error(state, err, line_offset: int = 0):
    divided_lines = [line for line in [err_line.strip() for err_line in err.split("\n")]]
    lines_with_line_number = [line for line in divided_lines if line.startswith("LINE ")]
    marker_lines = [line for line in [err_line for err_line in err.split("\n")] if line.strip() == "^"]

    print_buffer = dump_buffer(state, "")
    if len(lines_with_line_number) != 0:
        line_err_num = int(lines_with_line_number[0].split("LINE ")[1].split(":")[0])
        state.cur_file_line = line_err_num
        buffer_lines = state.fetched_query.strip().replace("\r\n", "\n").split("\n")
        start_line_num = max(0, line_err_num - 10) + 1
        end_line_num = min(line_err_num, len(buffer_lines)) + 1
        buffer_lines = buffer_lines[start_line_num - 1:end_line_num - 1]

        marker_line = marker_lines[0]
        marker_line = marker_line[lines_with_line_number[0].index(":"):]
        marker_line = "     " + marker_line

        new_err = "\n".join(err.replace("\r\n", "\n").split("\n")[:-4])
        if len(new_err) == 0:
            err = err.split("\n")[0]
        else:
            err = new_err

        buffer_lines = [str(start_line_num + idx).rjust(5, '0') + "> " +
                        (line + "\n" + marker_line + "\n" + err if start_line_num + idx == line_err_num else line)
                        for idx, line in zip(range(len(buffer_lines)), buffer_lines)]

        err = "\\<b>" + err + "\\</b>\n\n" + "\n".join(buffer_lines)
        err = err + "\n\n"
    get_message(state, err, line_offset, print_buffer)


def print_error(state, err, line_offset: int = 0):
    get_message(state, err, line_offset, dump_buffer(state, ""))


def freeze_defrost_instance(state: State, freeze: bool):
    endpoint = ENDPOINT__freeze if freeze else ENDPOINT__defrost
    verb = "freezing" if freeze else "defrosting"
    res = state.request_handler(METHOD__post, endpoint, handle_error=False)

    if res.status_code != 200:
        print_error(state, "Error " + verb + " jaaql box, received status code %d and message:\n\n\t%s" % (res.status_code, res.text))


def wipe_jaaql_box(state: State):
    res = state.request_handler(METHOD__post, ENDPOINT__wipe, handle_error=False)

    if res.status_code != 200:
        print_error(state, "Error wiping jaaql box, received status code %d and message:\n\n\t%s" % (res.status_code, res.text))


def set_web_config(state: State):
    res = state.request_handler(METHOD__post, ENDPOINT__set_web_config, handle_error=False)

    if res.status_code != 200:
        print_error(state, "Error setting web config, received status code %d and message:\n\n\t%s" % (res.status_code, res.text))


def attach_email_account(state, application: str, dispatcher_name: str, credentials_name: str, connection_info: ConnectionInfo):
    res = state.request_handler(METHOD__post, ENDPOINT__dispatchers, send_json={
        "application": application,
        "name": dispatcher_name,
        "url": connection_info.get_host(),
        "port": connection_info.get_port(),
        "username": connection_info.username,
        "password": connection_info.password
    }, handle_error=False)

    if res.status_code != 200:
        print_error(state, "Error attaching email account '%s' to dispatcher '%s', received status code %d and message:\n\n\t%s" %
                    (credentials_name, dispatcher_name, res.status_code, res.text))


def register_jaaql_account(state, credentials_name: str, connection_info: ConnectionInfo, clone_users: list[str] = None):
    send_json = {
        "username": connection_info.username,
        "password": connection_info.password,
        "attach_as": connection_info.username
    }
    endpoint = ENDPOINT__attach
    if clone_users is not None:
        send_json = {"accounts": [{
            "username": user,
            "password": None if state.clone_as_attach else connection_info.password,
            "attach_as": user
        } for user in clone_users]}
        endpoint = ENDPOINT__attach_batch

    res = state.request_handler(METHOD__post, endpoint, send_json=send_json, handle_error=False)

    if res.status_code != 200:
        print_error(state, "Error registering jaaql account '%s' with username '%s', received status code %d and message:\n\n\t%s" %
                    (credentials_name, connection_info.username, res.status_code, res.text))


def federate_jaaql_user_account(state, credentials_name: str, connection_info: ConnectionInfo, provider: str, tenant: str, sub: str):
    send_json = {
        "username": connection_info.username,
        "password": None if state.clone_as_attach else connection_info.password,
        "provider": provider,
        "tenant": tenant,
        "sub": sub,
        "attach_as": connection_info.username
    }
    endpoint = ENDPOINT__attach_batch
    res = state.request_handler(METHOD__post, endpoint, send_json={"accounts": [send_json]}, handle_error=False)

    if res.status_code != 200:
        print_error(state, "Error registering jaaql account '%s' with username '%s', received status code %d and message:\n\n\t%s" %
                    (credentials_name, connection_info.username, res.status_code, res.text))


def on_go(state):
    for parameter, value in state.parameters.items():
        state.fetched_query = state.fetched_query.replace("{{" + parameter + "}}", value)

    send_json = {"query": state.fetched_query}
    if state.query_parameters is not None:
        try:
            send_json["parameters"] = json.loads(state.query_parameters)
        except JSONDecodeError as ex:
            print_error(state, "You have messed up your parameters: " + str(ex))
    cur_conn = state.get_current_connection()
    if cur_conn.database is not None:
        send_json["database"] = cur_conn.database
    if state.database_override is not None:
        send_json["database"] = state.database_override
    if not state.is_transactional:
        send_json["autocommit"] = True
    if not state.prevent_unused_parameters:
        send_json["prevent_unused_parameters"] = False

    state.request_handler(METHOD__post, ENDPOINT__submit, send_json=send_json, line_offset=len(state.fetched_query.splitlines()) - 1)

    state.fetched_query = ""
    state.query_parameters = None


def parse_user_printing_any_errors(state, potential_user, allow_spaces: bool = False):
    if " " in potential_user and not allow_spaces:
        print_error(state, "Expected user without spaces, instead found spaces in user: '" + potential_user + "'")
    if not potential_user.startswith("@"):
        print_error(state, "Malformatted user, expected user to start with @")

    return potential_user.split("@")[1].split(" ")[0]


def deal_with_prepare(state: State, file_content: str = None, cost_only: bool = False):
    if len(state.connections) != 0 and state.connections.get(DEFAULT_CONNECTION):
        state.set_current_connection(get_connection_info(state, DEFAULT_CONNECTION), DEFAULT_CONNECTION)  # Preloads the default connection

    cur_conn = state.get_current_connection()

    send_json = {
        "queries": file_content,
        "database": cur_conn.database if cur_conn.database is not None else (state.database_override if state.database_override is not None else None),
        "cost_only": cost_only
    }

    state.request_handler(METHOD__post, ENDPOINT__prepare, send_json=send_json, format_as_query_output=False, compress_output_unless=["exception"])


def fire_cron(state: State, cron_application, cron_command, cron_args):
    try:
        cron_args = json.loads(cron_args)
        state.request_handler(METHOD__post, ENDPOINT__cron, send_json={**{
            "application": cron_application,
            "command": cron_command,
        }, **cron_args})
    except JSONDecodeError as ex:
        print(cron_args, file=sys.stderr)
        raise ex


def read_file_lines_with_fallback(file_path, first_encoding='utf-8-sig', second_encoding='windows-1252', default_encoding='utf-8'):
    try:
        # Attempt to open with the first specified encoding
        with open(file_path, 'r', encoding=first_encoding) as f:
            return f.readlines()
    except UnicodeDecodeError:
        try:
            with open(file_path, 'r', encoding=second_encoding) as f:
                return f.readlines()
        except UnicodeDecodeError as e:
            try:
                # Attempt to open with the first specified encoding
                with open(file_path, 'r', encoding=default_encoding) as f:
                    return f.readlines()
            except UnicodeDecodeError:
                raise e
            except Exception as e:
                raise e
        except Exception as e:
            raise e
    except Exception as e:
        raise e


def construct_docker_command(container_name, sql_file_path_inside_container, supplied_database):
    """
    Constructs the Docker command based on the operating system.
    """
    docker_exec = ["docker", "exec"]

    # If not Windows, prepend 'sudo'
    if not platform.system().lower() == 'windows':
        docker_exec = ["sudo"] + docker_exec

    # Full Docker command to execute psql
    command = docker_exec + [
        container_name,
        "psql",
        "-U", "postgres",
        "-d", supplied_database,
        "-f", sql_file_path_inside_container
    ]

    return command


def execute_command(state, command):
    """
    Executes the given command and returns the result.
    """
    try:
        result = subprocess.run(
            command,
            shell=False,
            check=False,  # We will handle errors manually
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True  # Decode to string
        )
        return result
    except Exception as e:
        print_error(state, f"Error executing command slurp in command: {e}")


def read_utf8_lines(state, filename):
    try:
        with open(filename, 'rb') as file:
            # Read the first few bytes to check for BOM
            first_bytes = file.read(3)

            # Check for UTF-8-SIG (BOM)
            if first_bytes.startswith(b'\xef\xbb\xbf'):
                # Reopen with 'utf-8-sig' to properly handle BOM
                file = open(filename, encoding='utf-8-sig')
            else:
                # Reopen with 'utf-8' assuming no BOM
                file = open(filename, encoding='utf-8')

            return file.readlines()
    except FileNotFoundError:
        print_error(state, "Could not locate file: " + filename)


def execute_file_with_psql(state: State, username, database, file_relative, file_absolute, url):
    insert_prior = f'SET SESSION AUTHORIZATION "{username}";'
    with open(file_absolute, 'r', encoding='utf-8-sig') as f_src, open(state.slurp_in_location + "/" + os.path.basename(file_relative), 'w',
                                                                       encoding='utf-8') as f_dest:
        f_dest.write(insert_prior + '\n')
        f_dest.write("SET client_min_messages=WARNING;\n")
        shutil.copyfileobj(f_src, f_dest)

    command = construct_docker_command("jaaql_pg" if "6060" in url else "jaaql_container", "/slurp-in/" + os.path.basename(file_relative), database)
    result = execute_command(state, command)

    if result.returncode != 0 or len(result.stderr) != 0:
        print_error(state, f"Error executing: {file_relative}\n\n{result.stderr}")


def deal_with_input(state: State, file_content: str = None):
    if len(state.connections) == 0 and state.is_script():
        print_error(state, "Must supply credentials file as argument in script mode")
    if len(state.connections) != 0 and state.connections.get(DEFAULT_CONNECTION):
        state.set_current_connection(get_connection_info(state, DEFAULT_CONNECTION), DEFAULT_CONNECTION)  # Preloads the default connection
    elif not state.is_script():
        print(state, "Type jaaql url or \"file [config_file_location]\"")
        state.set_current_connection(handle_login(state, input("LOGIN>").strip()))

    if state.is_script():
        try:
            if file_content:
                state.file_lines = [line + "\n" for line in file_content.replace("\r\n", "\n").split("\n")]
            else:
                state.file_lines = read_file_lines_with_fallback(state.file_name)
            state.file_lines.append(EOFMarker())  # Ignore warning. We can have multiple types. This is python
        except FileNotFoundError:
            print_error(state, "Could not load file for processing '" + state.file_name + "'")
        except Exception as ex:
            print_error(state, "Unhandled exception whilst processing file '" + state.file_name + "' " + str(ex))

    in_cron = False
    cron_application = None
    cron_command = None
    cron_args = ""

    while True:
        fetched_line = None

        try:
            if len(state.file_lines) != 0:
                while fetched_line is None:
                    fetched_line = state.file_lines[0]
                    state.cur_file_line += 1
                    state.file_lines = state.file_lines[1:]
                    if isinstance(fetched_line, EOFMarker):
                        if len(state.file_stack) == 0:
                            raise EOFError()
                        else:
                            last_ret = state.file_stack[-1]
                            state.cur_file_line = last_ret["cur_file_line"]
                            state.file_lines = last_ret["file_lines"]
                            state.file_name = last_ret["cur_file_name"]
                            state.file_stack = state.file_stack[:-1]
                            fetched_line = None
        except EOFError:
            break

        if fetched_line.startswith(COMMAND__initialiser) or fetched_line.upper().startswith(COMMAND__with_parameters) or fetched_line.upper().startswith(COMMAND__with_user):
            fetched_line = fetched_line.strip()  # Ignore the line terminator e.g. \r\n
            if fetched_line == COMMAND__go or fetched_line == COMMAND__go_short:
                on_go(state)
            elif fetched_line == COMMAND__reset or fetched_line == COMMAND__reset_short:
                state.fetched_query = ""
            elif fetched_line == COMMAND__print or fetched_line == COMMAND__print_short:
                dump_buffer(state)
            elif fetched_line == COMMAND__freeze_instance:
                freeze_defrost_instance(state, freeze=True)
            elif fetched_line == COMMAND__defrost_instance:
                freeze_defrost_instance(state, freeze=False)
            elif fetched_line.startswith(COMMAND__import):
                state.file_stack.append({
                    "cur_file_line": state.cur_file_line,
                    "file_lines": state.file_lines,
                    "cur_file_name": state.file_name
                })
                import_file = " ".join(fetched_line.split(COMMAND__import)[1:]).strip()
                if import_file.startswith("%TEMP%"):
                    import_file = import_file.replace("%TEMP%", tempfile.gettempdir().replace("\\", "/"))
                file_path = os.path.join(dirname(state.file_name), import_file)
                state.file_name = file_path
                state.file_lines = read_utf8_lines(state, state.file_name)
                state.file_lines.append(EOFMarker())
            elif len(state.fetched_query.strip()) != 0:
                print_error(state, "Tried to execute the command '" + fetched_line + "' but buffer was non empty.")
            elif fetched_line.startswith(COMMAND__cron):
                cron_command = fetched_line.split(COMMAND__cron + " ")[1].strip()
                cron_application = cron_command.split(" ")[0]
                cron_command = " ".join(cron_command.split(" ")[1:])
                cron_args = ""
                in_cron = True
            elif fetched_line == COMMAND__wipe_dbms:
                wipe_jaaql_box(state)
            elif fetched_line == COMMAND__set_web_config:
                set_web_config(state)
            elif fetched_line.upper().startswith(COMMAND__with_user):
                overriding_user = fetched_line.upper().split(COMMAND__with_user)[1].strip().lower()
                if "\"" in overriding_user or "'" in overriding_user:
                    print_error(state, "Please do not quote the user!")
                state.get_current_connection().username = overriding_user
            elif fetched_line.startswith(COMMAND__with_parameters):
                if fetched_line.strip().endswith("}"):
                    state.query_parameters = fetched_line[len(COMMAND__with_parameters)-1:]
                else:
                    state.query_parameters = "{"
                    state.reading_parameters = True
            elif fetched_line.startswith(COMMAND__switch_jaaql_account_to):
                candidate_connection_name = fetched_line.split(COMMAND__switch_jaaql_account_to)[1]
                connection_name = parse_user_printing_any_errors(state, candidate_connection_name)
                state.set_current_connection(get_connection_info(state, connection_name=connection_name), connection_name)
            elif fetched_line.startswith(COMMAND__connect_to_database):
                candidate_database = fetched_line.split(COMMAND__connect_to_database)[1].split(" ")[0]
                if fetched_line.endswith(CONNECT_FOR_CREATEDB) or fetched_line.startswith(CONNECT_FOR_EXTENSION_CONFIGURATION):
                    state.is_transactional = False

                state.database_override = candidate_database.split(CONNECT_FOR_CREATEDB)[0].split(CONNECT_FOR_EXTENSION_CONFIGURATION)[0]
            elif fetched_line.startswith(COMMAND__clone_jaaql_account):
                candidate_connection_name = fetched_line.split(COMMAND__clone_jaaql_account)[1].split(" ")[0]
                connection_name = parse_user_printing_any_errors(state, candidate_connection_name)
                federation_data = json.loads(" ".join(fetched_line.split(COMMAND__clone_jaaql_account)[1].split(" ")[1:]))

                connection_info = get_connection_info(state, connection_name=connection_name)
                federate_jaaql_user_account(state, connection_name, connection_info, federation_data["provider"], federation_data["tenant"],
                                            federation_data["sub"])
            elif fetched_line.startswith(COMMAND__register_jaaql_account_with):
                candidate_connection_name = fetched_line.split(COMMAND__register_jaaql_account_with)[1].split(" ")[0]
                overriding = fetched_line.split(" overriding username as ")
                if len(overriding) != 1:
                    overriding = overriding[1]
                else:
                    overriding = None
                connection_name = parse_user_printing_any_errors(state, candidate_connection_name)

                register_jaaql_account(state, connection_name, get_connection_info(state, connection_name=connection_name, override_username=overriding))
            elif fetched_line.startswith(COMMAND__federate_jaaql_account_with):
                candidate_connection_name = fetched_line.split(COMMAND__federate_jaaql_account_with)[1].split(" ")[0]
                connection_name = parse_user_printing_any_errors(state, candidate_connection_name)
                federation_data = json.loads(" ".join(fetched_line.split(COMMAND__federate_jaaql_account_with)[1].split(" ")[1:]))
                connection_info = get_connection_info(state, connection_name=connection_name, override_username=federation_data.username)
                federate_jaaql_user_account(state, connection_name, connection_info, federation_data["provider"], federation_data["tenant"],
                                            federation_data["sub"])
            elif fetched_line.startswith(COMMAND__attach_email_account):
                candidate_connection_name = fetched_line.split(COMMAND__attach_email_account)[1]
                connection_name = parse_user_printing_any_errors(state, candidate_connection_name, allow_spaces=True)
                if " to " not in candidate_connection_name:
                    print_error(state, "Expected token 'to' after dispatcher credentials file e.g. " +
                                COMMAND__attach_email_account + "@dispatcher to app.dispatcher_name")
                if candidate_connection_name.endswith(" to "):
                    print_error(state, "Expected fully qualified dispatcher after ' to ' e.g. " +
                                COMMAND__attach_email_account + "@dispatcher to app.dispatcher_name")
                dispatcher_fqn = candidate_connection_name.split(" to ")[1]
                dispatcher_fqn_split = dispatcher_fqn.split(".")
                if len(dispatcher_fqn_split) != 2:
                    print_error(state, "Badly formatted dispatcher name. Must be of the format 'app.dispatcher_name'. Received '%s'" % dispatcher_fqn)

                attach_email_account(state, dispatcher_fqn_split[0], dispatcher_fqn_split[1], connection_name,
                                     get_connection_info(state, connection_name=connection_name))
            elif fetched_line.startswith(COMMAND__psql):
                the_user = parse_user_printing_any_errors(state, fetched_line.split(COMMAND__psql)[1].split(" ")[0])
                the_file = fetched_line.split(COMMAND__psql)[1].split(" ")[1]

                if state.slurp_in_location is None:
                    print_error(state, "No 'slurp in location' provided. Use arg '%s'" % ARGS__slurp_in_location[0])

                connection = get_connection_info(state, connection_name=the_user)
                file_path = os.path.join(dirname(state.file_name), the_file)
                execute_file_with_psql(state, connection.username, connection.database, the_file, file_path, connection.get_http_url())
            elif fetched_line == COMMAND__quit or fetched_line == COMMAND__quit_short:
                break
            else:
                print_error(state, "Unrecognised command '" + fetched_line + "'")
        elif in_cron:
            cron_args += fetched_line.strip()
            if cron_args.endswith("}"):
                in_cron = False
                fire_cron(state, cron_application, cron_command, cron_args)
        else:
            if state.reading_parameters:
                if fetched_line.strip().startswith("}"):
                    state.reading_parameters = False

                    if len(fetched_line.split("} ")) > 1 and (fetched_line.split("} ")[1].upper().startswith(COMMAND__and_user) or fetched_line.split("} ")[1].upper().startswith(COMMAND__with_user)):
                        delimiter = COMMAND__and_user if COMMAND__and_user in fetched_line.upper() else COMMAND__with_user
                        overriding_user = fetched_line.upper().split(delimiter)[1].strip().lower()
                        if "\"" in overriding_user or "'" in overriding_user:
                            print_error(state, "Please do not quote the user!")
                        state.get_current_connection().username = overriding_user

                    state.query_parameters += "}"
                else:
                    state.query_parameters += fetched_line
            else:
                if len(state.fetched_query.strip()) != 0 or len(fetched_line.strip()) != 0:
                    state.fetched_query += fetched_line  # Do not pre-append things with empty lines

            if fetched_line.strip().endswith(COMMAND__go_short):
                if state.reading_parameters:
                    state.query_parameters = state.query_parameters[:-(len(COMMAND__go_short) + 1)]
                    state.reading_parameters = False
                else:
                    state.fetched_query = state.fetched_query[:-(len(COMMAND__go_short) + 1)]

                on_go(state)

    if len(state.fetched_query) != 0:
        if state.single_query:
            on_go(state)
        else:
            print_error(state, "Attempting to quit with non-empty buffer. Please submit with \\g or clear with \\r")


def initialise_from_args(args, file_name: str = None, file_content: str = None, do_exit: bool = True, override_url: str = None, do_prepare: bool = False):
    state = State()
    state.do_exit = do_exit

    if file_name is None:
        for idx, arg in zip(range(len(args)), args):
            if arg in ARGS__input_file:
                state.future_files.append({"name": args[idx + 1], "type": FUTURE_TYPE_input})
            elif arg in ARGS__psql_file:
                state.future_files.append({"name": args[idx + 1], "type": FUTURE_TYPE_psql})

        file_name = [idx for arg, idx in zip(args, range(len(args))) if arg in ARGS__input_file]
        if len(file_name) != 0:
            state.file_name = args[file_name[0] + 1]
    else:
        state.file_name = file_name

    state.override_url = override_url

    state.is_verbose = len([arg for arg in args if arg in ['-v', '--verbose']]) != 0
    state.skip_auth = len([arg for arg in args if arg in ARGS__skip_auth]) != 0
    state.is_debugging = len([arg for arg in args if arg in ['-d', '--debugging']]) != 0
    state.single_query = len([arg for arg in args if arg in ARGS__single_query]) != 0
    state.prevent_unused_parameters = len([arg for arg in args if arg in ARGS__allow_unused_parameters]) == 0
    state.clone_as_attach = len([arg for arg in args if arg in ARGS__clone_as_attach]) == 1

    cost_only = len([arg for arg in args if arg in ARGS__cost_only]) == 1

    if state.is_verbose:
        print_version()

    for arg, arg_idx in zip(args, range(len(args))):
        if arg in ARGS__slurp_in_location:
            if arg_idx == len(args) - 1:
                print_error(state, "The slurp in arg is the last argument. You need to supply a directory")
            state.slurp_in_location = args[arg_idx + 1]
            for item in os.listdir(state.slurp_in_location):
                item_path = os.path.join(state.slurp_in_location, item)
                if os.path.isfile(item_path) or os.path.islink(item_path):
                    os.unlink(item_path)
                elif os.path.isdir(item_path):
                    shutil.rmtree(item_path)

        if arg not in ARGS__parameter:
            continue

        if arg_idx == len(args) - 1:
            print_error(state, "The parameter flag is the last argument. You need to supply a parameter name")

        if arg_idx == len(args) - 2:
            print_error(state, "The parameter name is the last argument. You need to supply a parameter value")

        parameter_name = args[arg_idx + 1]
        parameter_value = args[arg_idx + 2]

        if parameter_name in state.parameters:
            print_error(state, "The parameter '" + parameter_name + "' has already been supplied")

        state.parameters[parameter_name] = parameter_value

    for arg, arg_idx in zip(args, range(len(args))):
        if arg not in ARGS__environment:
            continue

        if arg_idx == len(args) - 1:
            print_error(state, "The environment flag is the last argument. You need to supply a file")

        parameter_file = args[arg_idx + 1]

        for line in open(parameter_file, "r").readlines():
            state.parameters[line.split("=")[0]] = "=".join(line.split("=")[1:])

    for arg, arg_idx in zip(args, range(len(args))):
        if arg not in ARGS__encoded_config and arg not in ARGS__config:
            continue

        if arg_idx == len(args) - 1:
            print_error(state, "The config flag is the last argument. You need to supply a file")

        configuration_name = args[arg_idx + 1]
        candidate_content_or_file_name = None
        if arg_idx < len(args) - 2:
            candidate_content_or_file_name = args[arg_idx + 2]

        # The following branch of logic will use the supplied configuration name as the file name and set the configuration name to default
        if candidate_content_or_file_name is None or candidate_content_or_file_name.startswith("<") or candidate_content_or_file_name.startswith("-"):
            candidate_content_or_file_name = configuration_name
            configuration_name = DEFAULT_CONNECTION

        if configuration_name in state.connections:
            print_error(state, "The configuration with name '" + configuration_name + "' already exists")

        state.connections[configuration_name] = candidate_content_or_file_name

        if arg in ARGS__encoded_config:
            content_split = candidate_content_or_file_name.split(":")

            db = None
            if len(content_split) == 4:
                db = b64d(content_split[3]).decode()

            state.connection_info[configuration_name] = ConnectionInfo(b64d(content_split[0]).decode(), b64d(content_split[1]).decode(),
                                                                       b64d(content_split[2]).decode(), db, state.override_url)

    for arg, arg_idx in zip(args, range(len(args))):
        if arg not in ARGS__folder_config:
            continue

        if arg_idx == len(args) - 1:
            print_error(state, "The folder config flag is the last argument. You need to supply a file")

        configuration_folder = args[arg_idx + 1]

        for sub_folder in configuration_folder.split(";"):
            for config_file in os.listdir(sub_folder):
                full_file_name = os.path.join(sub_folder, config_file)
                if config_file.endswith(".email-credentials.txt"):
                    configuration_name = config_file[0:-len(".email-credentials.txt")]
                elif config_file.endswith(".credentials.txt"):
                    configuration_name = config_file[0:-len(".credentials.txt")]
                else:
                     # raise JAAQLMonitorException("Unrecognised file extension for file " + full_file_name)
                    continue  # Just ignore, it's not a problem

                # We replace it now!
                # if configuration_name in state.connections:
                #     continue  # Allow this

                state.connections[configuration_name] = full_file_name

    if do_prepare:
        deal_with_prepare(state, file_content, cost_only=cost_only)
    else:
        if file_content is not None:
            deal_with_input(state, file_content)
        else:
            while (future := state.get_next()) != FUTURE_TYPE_none:
                state.file_name = future['name']
                if future['type'] == FUTURE_TYPE_input:
                    deal_with_input(state, file_content)
                else:
                    default_connection = get_connection_info(state, connection_name=DEFAULT_CONNECTION)
                    exec_as = "dba" if state.file_name.endswith("dba") else "jaaql"
                    the_database = default_connection.database
                    try:
                        the_database = get_connection_info(state, connection_name="dba_db" if state.file_name.endswith("dba") else "jaaql").database
                        exec_as = "dba" if state.file_name.endswith("dba") else "jaaql"
                    except:
                        pass
                    execute_file_with_psql(state, exec_as, the_database, state.file_name, state.file_name, default_connection.get_http_url())


def initialise(file_name: str, configs: list[[str, str]], encoded_configs: list[[str, str, str, str, str | None]],
               override_url: str, folder_name: str = None, do_prepare: bool = False, file_content: str | None = None, additional_args: list = None):
    args = [ARGS__single_query[0]]
    if additional_args is not None:
        args = args + additional_args

    for config in configs:
        args.append(ARGS__config[0])
        args.append(config[0])
        args.append(config[1])

    if folder_name is not None:
        args.append(ARGS__folder_config[0])
        args.append(folder_name)

    for encoded_config in encoded_configs:
        args.append(ARGS__encoded_config[0])
        args.append(encoded_config[0])
        db_part = ""
        if encoded_config[4]:
            db_part = ":" + b64e(encoded_config[4].encode()).decode()
        args.append(b64e(encoded_config[1].encode()).decode() + ":" + b64e(encoded_config[2].encode()).decode() + ":" +
                    b64e(encoded_config[3].encode()).decode() + db_part)

    initialise_from_args(args, file_name, file_content, False, override_url, do_prepare=do_prepare)


if __name__ == "__main__":
    initialise_from_args(sys.argv[1:])
