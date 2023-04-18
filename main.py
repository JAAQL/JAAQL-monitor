from version import print_version
import sys
import requests
from sys import exit
from getpass import getpass
from inspect import getframeinfo, stack
from datetime import datetime
import os

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


class EOFMarker:
    pass


class ConnectionInfo:
    def __init__(self, host, username, password, database):
        self.host = host
        self.username = username
        self.password = password
        self.database = database
        self.oauth_token = None

    def get_port(self):
        return int(self.host.split(":")[1])

    def get_host(self):
        return self.host.split(":")[0]

    def get_http_url(self):
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

            conn.oauth_token = {"Authentication-Token": oauth_res.json()}
        except requests.exceptions.RequestException:
            print_error(self, "Could not connect to JAAQL running on " + conn.host + "\nPlease make sure that JAAQL is running and accessible")

    def time_delta_ms(self, start_time: datetime, end_time: datetime) -> int:
        return int(round((end_time - start_time).total_seconds() * 1000))

    def request_handler(self, method, endpoint, send_json=None, handle_error: bool = True):
        conn = self.get_current_connection()
        if conn.oauth_token is None:
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

        ci = ConnectionInfo(host, username, password, database)

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
        col_str = str(col)
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

    return ConnectionInfo(jaaql_url, username, password, None)


def dump_buffer(state, start: str = "\n\n"):
    return ("%sBuffer was length " % start) + str(len(state.fetched_query.strip())) + " with contents:\n" + state.fetched_query.strip() + "\n\n"


def print_error(state, err, line_offset: int = 0):
    caller = getframeinfo(stack()[1][0])
    file_message = ""
    if state.file_name is not None:
        file_message = "Error on line %d of file '%s':\n\n" % (state.cur_file_line - line_offset, state.file_name)
    debug_message = " [%s:%d]" % (caller.filename, caller.lineno)
    if not state.is_script() or not state.is_debugging:
        debug_message = ""
    buffer = "\n" + dump_buffer(state, "")
    if not state.is_script():
        buffer = ""
    print(file_message + err + debug_message + buffer, file=sys.stderr)
    if state.is_script():
        exit(1)


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
    send_json = {"query": state.fetched_query}
    cur_conn = state.get_current_connection()
    if cur_conn.database is not None:
        send_json["database"] = cur_conn.database
    if state.database_override is not None:
        send_json["database"] = state.database_override
    if not state.is_transactional:
        send_json["autocommit"] = True

    state.request_handler(METHOD__post, ENDPOINT__submit, send_json=send_json)


def parse_user_printing_any_errors(state, potential_user, allow_spaces: bool = False):
    if " " in potential_user and not allow_spaces:
        print_error(state, "Expected user without spaces, instead found spaces in user: '" + potential_user + "'")
    if not potential_user.startswith("@"):
        print_error(state, "Malformatted user, expected user to start with @")

    return potential_user.split("@")[1].split(" ")[0]


def deal_with_input(state: State):
    if len(state.connections) == 0 and state.is_script():
        print_error(state, "Must supply credentials file as argument in script mode")
    if len(state.connections) != 0 and state.connections.get(DEFAULT_CONNECTION):
        state.set_current_connection(get_connection_info(state, DEFAULT_CONNECTION))  # Preloads the default connection
    elif not state.is_script():
        print(state, "Type jaaql url or \"file [config_file_location]\"")
        state.set_current_connection(handle_login(state, input("LOGIN>").strip()))

    if state.is_script():
        try:
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
                state.fetched_query = ""
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

    if len(state.fetched_query) != 0:
        if state.single_query:
            on_go(state)
        else:
            print_error(state, "Attempting to quit with non-empty buffer. Please submit with \\g or clear with \\r")


def initialise_from_args():
    state = State()

    args = sys.argv[1:]

    file_name = [idx for arg, idx in zip(args, range(len(args))) if arg in ['-f', '--file']]
    if len(file_name) != 0:
        state.file_name = args[file_name[0] + 1]

    state.is_verbose = len([arg for arg in args if arg in ['-v', '--verbose']]) != 0
    state.is_debugging = len([arg for arg in args if arg in ['-d', '--debugging']]) != 0
    state.single_query = len([arg for arg in args if arg in ['-s', '--single-query']]) != 0

    if state.is_verbose:
        print_version()

    has_config = len([arg for arg in args if arg in ['-c', '--config']]) != 0
    if has_config:
        for arg, arg_idx in zip(args, range(len(args))):
            if arg not in ['-c', '--config']:
                continue
            if arg_idx == len(args) - 1:
                print_error(state, "The config flag is the last argument. You need to supply a file")
            configuration_name = args[arg_idx + 1]
            candidate_file_name = None
            if arg_idx < len(args) - 2:
                candidate_file_name = args[arg_idx + 2]

            # The following branch of logic will use the supplied configuration name as the file name and set the configuration name to default
            if candidate_file_name is None or candidate_file_name.startswith("<") or candidate_file_name.startswith("-"):
                candidate_file_name = configuration_name
                configuration_name = DEFAULT_CONNECTION

            if configuration_name in state.connections:
                print_error(state, "The configuration with name '" + configuration_name + "' already exists")

            state.connections[configuration_name] = candidate_file_name

    deal_with_input(state)


if __name__ == "__main__":
    initialise_from_args()
