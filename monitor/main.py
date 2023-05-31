from monitor.version import print_version
import sys
import requests
from sys import exit
from getpass import getpass
from inspect import getframeinfo, stack
from datetime import datetime
from base64 import urlsafe_b64encode as b64e, urlsafe_b64decode as b64d
import os

HEADER__security_bypass = "Authentication-Token-Bypass"
HEADER__security_bypass_jaaql = "Authentication-Token-Bypass-Jaaql"
HEADER__security = "Authentication-Token"
MARKER__bypass = "bypass "
MARKER__jaaql_bypass = "jaaql_bypass "

ENDPOINT__oauth = "/oauth/token"
ENDPOINT__submit = "/submit"
ENDPOINT__attach = "/accounts"
ENDPOINT__dispatchers = "/internal/dispatchers"
ENDPOINT__wipe = "/internal/clean"

COMMAND__initialiser = "\\"
COMMAND__reset_short = "\\r"
COMMAND__reset = "\\reset"
COMMAND__go_short = "\g"
COMMAND__go = "\go"
COMMAND__print_short = "\p"
COMMAND__print = "\print"
COMMAND__wipe_dbms = "\wipe dbms"
COMMAND__switch_jaaql_account_to = "\switch jaaql account to "
COMMAND__connect_to_database = "\connect to database "
COMMAND__register_jaaql_account_with = "\\register jaaql account with "
COMMAND__attach_email_account = "\\attach email account "
COMMAND__quit_short = "\q"
COMMAND__quit = "\quit"

CONNECT_FOR_CREATEDB = " for createdb"

DEFAULT_CONNECTION = "default"
DEFAULT_EMAIL_ACCOUNT = "default"

LINE_LENGTH_MAX = 115
ROWS_MAX = 25

METHOD__post = "POST"
METHOD__get = "GET"

ARGS__encoded_config = ['-e', '--encoded-config']
ARGS__config = ['-c', '--config']
ARGS__folder_config = ['-f', '--folder-config']
ARGS__parameter = ['-p', '--parameter']


class JAAQLMonitorException(Exception):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class EOFMarker:
    pass


class ConnectionInfo:
    def __init__(self, host, username, password, database, override_url = None):
        self.host = host
        self.username = username
        self.password = password
        self.database = database
        self.oauth_token = None
        self.override_url = override_url

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


class State:
    def __init__(self):
        self.was_go = False
        self.fetched_query = ""
        self.fetched_stdin = None
        self.connections = {}
        self.connection_info = {}
        self._current_connection = None
        self.fetched_database = None
        self.is_verbose = False
        self.single_query = False
        self.is_debugging = False
        self.file_name = None
        self.cur_file_line = 0
        self.file_lines = []
        self.override_url = None
        self.parameters = {}

        self.do_exit = True

        self.database_override = None
        self.is_transactional = True

    def set_current_connection(self, connection: ConnectionInfo):
        self._current_connection = connection
        self.database_override = None
        self.is_transactional = True

    def is_script(self):
        return self.file_name is not None

    def get_current_connection(self) -> ConnectionInfo:
        if self._current_connection is None:
            print_error(self, "There is no selected connection. Please supply a default connection or switch to a connection first")

        return self._current_connection

    def log(self, msg):
        if self.is_verbose:
            print(str(msg))

    def _fetch_oauth_token_for_current_connection(self):
        conn = self.get_current_connection()
        try:
            oauth_res = requests.post(conn.get_http_url() + ENDPOINT__oauth, json={
                "username": conn.username,
                "password": conn.password
            })

            if oauth_res.status_code != 200:
                print_error(self, "Invalid credentials: response code " + str(oauth_res.status_code) + " content: " + oauth_res.text)
                return None

            conn.oauth_token = {HEADER__security: oauth_res.json()}
        except requests.exceptions.RequestException:
            print_error(self, "Could not connect to JAAQL running on " + conn.host + "\nPlease make sure that JAAQL is running and accessible")

    def time_delta_ms(self, start_time: datetime, end_time: datetime) -> int:
        return int(round((end_time - start_time).total_seconds() * 1000))

    def request_handler(self, method, endpoint, send_json=None, handle_error: bool = True):
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

        self.log("Request took " + str(self.time_delta_ms(start_time, datetime.now())) + "ms")

        if res.status_code == 200:
            format_query_output(self, res.json())
        else:
            if handle_error:
                if endpoint == ENDPOINT__submit:
                    submit_error(self, res.text)
                else:
                    print_error(self, res.text)

        return res


def split_by_lines(split_str, gap=1):
    split_str = split_str.split("".join(["\r\n"] * gap))
    if len(split_str) == 1:
        split_str = split_str[0].split("".join(["\n"] * gap))
    return [s for s in split_str if len(s.strip()) != 0]


def get_connection_info(state: State, connection_name: str = None, file_name: str = None):
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
        file_message = "Error on " + additional_line_message + "line %d of file '%s':\n\n" % (state.cur_file_line - line_offset, state.file_name)
    debug_message = " [%s:%d]" % (caller.filename, caller.lineno)
    if not state.is_script() or not state.is_debugging:
        debug_message = ""
    buffer = "\n" + buffer
    if not state.is_script():
        buffer = ""

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

        err = "\n".join(err.replace("\r\n", "\n").split("\n")[:-4])

        buffer_lines = [str(start_line_num + idx).rjust(5, '0') + "> " +
                        (line + "\n" + marker_line + "\n" + err if start_line_num + idx == line_err_num else line)
                        for idx, line in zip(range(len(buffer_lines)), buffer_lines)]

        err = "\\<b>" + err + "\\</b>\n\n" + "\n".join(buffer_lines)
        err = err + "\n\n"
    get_message(state, err, line_offset, print_buffer)


def print_error(state, err, line_offset: int = 0):
    get_message(state, err, line_offset, dump_buffer(state, ""))


def wipe_jaaql_box(state: State):
    res = state.request_handler(METHOD__post, ENDPOINT__wipe, handle_error=False)

    if res.status_code != 200:
        print_error(state, "Error wiping jaaql box, received status code %d and message:\n\n\t%s" % (res.status_code, res.text))


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


def register_jaaql_account(state, credentials_name: str, connection_info: ConnectionInfo):
    res = state.request_handler(METHOD__post, ENDPOINT__attach, send_json={
        "username": connection_info.username,
        "password": connection_info.password,
        "attach_as": connection_info.username
    }, handle_error=False)

    if res.status_code != 200:
        print_error(state, "Error registering jaaql account '%s' with username '%s', received status code %d and message:\n\n\t%s" %
                    (credentials_name, connection_info.username, res.status_code, res.text))


def on_go(state):
    for parameter, value in state.parameters.items():
        state.fetched_query = state.fetched_query.replace("{{" + parameter + "}}", value)

    send_json = {"query": state.fetched_query}
    cur_conn = state.get_current_connection()
    if cur_conn.database is not None:
        send_json["database"] = cur_conn.database
    if state.database_override is not None:
        send_json["database"] = state.database_override
    if not state.is_transactional:
        send_json["autocommit"] = True

    state.request_handler(METHOD__post, ENDPOINT__submit, send_json=send_json)

    state.fetched_query = ""


def parse_user_printing_any_errors(state, potential_user, allow_spaces: bool = False):
    if " " in potential_user and not allow_spaces:
        print_error(state, "Expected user without spaces, instead found spaces in user: '" + potential_user + "'")
    if not potential_user.startswith("@"):
        print_error(state, "Malformatted user, expected user to start with @")

    return potential_user.split("@")[1].split(" ")[0]


def deal_with_input(state: State, file_content: str = None):
    if len(state.connections) == 0 and state.is_script():
        print_error(state, "Must supply credentials file as argument in script mode")
    if len(state.connections) != 0 and state.connections.get(DEFAULT_CONNECTION):
        state.set_current_connection(get_connection_info(state, DEFAULT_CONNECTION))  # Preloads the default connection
    elif not state.is_script():
        print(state, "Type jaaql url or \"file [config_file_location]\"")
        state.set_current_connection(handle_login(state, input("LOGIN>").strip()))

    if state.is_script():
        try:
            if file_content:
                state.file_lines = [line + "\n" for line in file_content.replace("\r\n", "\n").split("\n")]
            else:
                state.file_lines = open(state.file_name, "r").readlines()
            state.file_lines.append(EOFMarker())  # Ignore warning. We can have multiple types. This is python
        except FileNotFoundError as ex:
            print_error(state, "Could not load file for processing '" + state.file_name + "'")
        except Exception as ex:
            print_error(state, "Unhandled exception whilst processing file '" + state.file_name + "' " + str(ex))

    while True:
        fetched_line = None
        try:
            if len(state.file_lines) != 0:
                fetched_line = state.file_lines[0]
                state.cur_file_line += 1
                state.file_lines = state.file_lines[1:]
                if isinstance(fetched_line, EOFMarker):
                    raise EOFError()
        except EOFError:
            break

        if fetched_line.startswith(COMMAND__initialiser):
            fetched_line = fetched_line.strip()  # Ignore the line terminator e.g. \r\n
            if fetched_line == COMMAND__go or fetched_line == COMMAND__go_short:
                on_go(state)
            elif fetched_line == COMMAND__reset or fetched_line == COMMAND__reset_short:
                state.fetched_query = ""
            elif fetched_line == COMMAND__print or fetched_line == COMMAND__print_short:
                dump_buffer(state)
            elif len(state.fetched_query.strip()) != 0:
                print_error(state, "Tried to execute the command '" + fetched_line + "' but buffer was non empty.")
            elif fetched_line == COMMAND__wipe_dbms:
                wipe_jaaql_box(state)
            elif fetched_line.startswith(COMMAND__switch_jaaql_account_to):
                candidate_connection_name = fetched_line.split(COMMAND__switch_jaaql_account_to)[1]
                connection_name = parse_user_printing_any_errors(state, candidate_connection_name)
                state.set_current_connection(get_connection_info(state, connection_name=connection_name))
            elif fetched_line.startswith(COMMAND__connect_to_database):
                candidate_database = fetched_line.split(COMMAND__connect_to_database)[1].split(" ")[0]
                if fetched_line.endswith(CONNECT_FOR_CREATEDB):
                    state.is_transactional = False

                state.database_override = candidate_database.split(CONNECT_FOR_CREATEDB)[0]
            elif fetched_line.startswith(COMMAND__register_jaaql_account_with):
                candidate_connection_name = fetched_line.split(COMMAND__register_jaaql_account_with)[1]
                connection_name = parse_user_printing_any_errors(state, candidate_connection_name)

                register_jaaql_account(state, connection_name, get_connection_info(state, connection_name=connection_name))
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
            elif fetched_line == COMMAND__quit or fetched_line == COMMAND__quit_short:
                break
            else:
                print_error(state, "Unrecognised command '" + fetched_line + "'")
        else:
            if len(state.fetched_query.strip()) != 0 or len(fetched_line.strip()) != 0:
                state.fetched_query += fetched_line  # Do not pre-append things with empty lines

            if fetched_line.strip().endswith(COMMAND__go_short):
                state.fetched_query = state.fetched_query[:-(len(COMMAND__go_short) + 1)]
                on_go(state)

    if len(state.fetched_query) != 0:
        if state.single_query:
            on_go(state)
        else:
            print_error(state, "Attempting to quit with non-empty buffer. Please submit with \\g or clear with \\r")


def initialise_from_args(args, file_name: str = None, file_content: str = None, do_exit: bool = True, override_url: str = None):
    state = State()
    state.do_exit = do_exit

    if file_name is None:
        file_name = [idx for arg, idx in zip(args, range(len(args))) if arg in ['-f', '--file']]
        if len(file_name) != 0:
            state.file_name = args[file_name[0] + 1]
    else:
        state.file_name = file_name

    state.override_url = override_url

    state.is_verbose = len([arg for arg in args if arg in ['-v', '--verbose']]) != 0
    state.is_debugging = len([arg for arg in args if arg in ['-d', '--debugging']]) != 0
    state.single_query = len([arg for arg in args if arg in ['-s', '--single-query']]) != 0

    if state.is_verbose:
        print_version()

    for arg, arg_idx in zip(args, range(len(args))):
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
        if arg not in ARGS__folder_config:
            continue

        if arg_idx == len(args) - 1:
            print_error(state, "The folder config flag is the last argument. You need to supply a file")

        configuration_folder = args[arg_idx + 1]

        for config_file in os.listdir(configuration_folder):
            full_file_name = os.path.join(configuration_folder, config_file)
            if config_file.endswith(".email-credentials.txt"):
                configuration_name = config_file[0:-len(".email.credentials.txt")]
            elif config_file.endswith(".credentials.txt"):
                configuration_name = config_file[0:-len(".credentials.txt")]
            else:
                raise JAAQLMonitorException("Unrecognised file extension for file " + full_file_name)

            if configuration_name in state.connections:
                print_error(state, "The configuration with name '" + configuration_name + "' already exists")

            state.connections[configuration_name] = full_file_name

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

    deal_with_input(state, file_content)


def initialise(file_name: str, file_content: str, configs: list[[str, str]], encoded_configs: list[[str, str, str, str, str | None]],
               override_url: str):
    args = ["-s"]

    for config in configs:
        args.append("-c")
        args.append(config[0])
        args.append(config[1])

    for encoded_config in encoded_configs:
        args.append("-e")
        args.append(encoded_config[0])
        db_part = ""
        if encoded_config[4]:
            db_part = ":" + b64e(encoded_config[4].encode()).decode()
        args.append(b64e(encoded_config[1].encode()).decode() + ":" + b64e(encoded_config[2].encode()).decode() + ":" +
                    b64e(encoded_config[3].encode()).decode() + db_part)

    initialise_from_args(args, file_name, file_content, False, override_url)


if __name__ == "__main__":
    initialise_from_args(sys.argv[1:])
