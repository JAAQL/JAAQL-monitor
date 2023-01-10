import sys
import requests
import json
from sys import exit
from getpass import getpass
from inspect import getframeinfo, stack

import urllib

ENDPOINT__oauth = "/oauth/token"
ENDPOINT__submit_file = "/submit"
ENDPOINT__submit = "/submit"

COMMAND__exit = "\q"
COMMAND__print = "\p"
COMMAND__go = "\g"
COMMAND__mode = "\m"
COMMAND__help = "\h"
COMMAND__switch = "\s"
COMMAND__login = "\l"
COMMAND__reset = "\\r"
COMMAND__input = "\i"

DEFAULT_CONNECTION = "default"

MODE__sql = "sql"
MODE__jaaql = "jaaql"
ALLOWED_MODES = [MODE__sql, MODE__jaaql]

LINE_LENGTH_MAX = 115
ROWS_MAX = 25


class EOFMarker:
    pass


def fetch_oauth_token(jaaql_url, tenant, username, password, was_login):
    oauth_res = requests.post(jaaql_url + ENDPOINT__oauth, json={
        "tenant": tenant,
        "username": username,
        "password": password
    })

    if oauth_res.status_code != 200:
        print_error("Invalid credentials: response code " + str(oauth_res.status_code) + " content: " + oauth_res.text, was_login)
        return None

    return {"Authentication-Token": oauth_res.json()}


def split_by_lines(split_str, gap=1):
    split_str = split_str.split("".join(["\r\n"] * gap))
    if len(split_str) == 1:
        split_str = split_str[0].split("".join(["\n"] * gap))
    return split_str


def format_url(url_input: str):
    url_input = url_input.strip()
    if not url_input.startswith("http"):
        url_input = "https://www." + url_input
        if not url_input.endswith("/api"):
            url_input += "/api"
    if url_input.startswith("http") and ":6060" not in url_input and not url_input.endswith("/api"):
        url_input += "/api"
    return url_input


def load_from_config_file(file_name, credentials_name: str = None):
    global connection_info

    if credentials_name and credentials_name in connection_info:
        return connection_info[credentials_name]

    try:
        config = open(file_name, "r").read()
        config = split_by_lines(config)
        jaaql_url = format_url(config[0])
        tenant = config[1].strip()
        username = config[2].strip()
        password = config[3].strip()
        database = None
        if len(config) > 4:
            database = config[4].strip()
            if len(database) == 0:
                database = None
            else:
                log("Found database '" + database + "'")

        log("Successfully loaded config")

        if credentials_name:
            connection_info[credentials_name] = jaaql_url, tenant, username, password, database

        return jaaql_url, tenant, username, password, database
    except FileNotFoundError:
        if credentials_name is None:
            print_error("Could not find credentials file located at '" + file_name + "'", True)
        else:
            print_error("Could not find named credentials file '" + credentials_name + "' located at '" + file_name + "'", True)


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


def format_query_output(json_output):
    if "rows" not in json_output:
        return None
    str_num_rows = "(" + str(len(json_output["rows"])) + " " + ("row" if len(json_output["rows"]) == 1 else "rows") + ")"

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

    log(format_output_divider(max_length))
    log(format_output_row(json_output["columns"], max_length, [str] * len(json_output["columns"]), [False] * len(max_length)))
    log(format_output_divider(max_length))

    if len(json_output["rows"]) > ROWS_MAX:
        json_output["rows"] = json_output["rows"][0:ROWS_MAX]
        json_output["rows"].append(["..." for _ in json_output["columns"]])

    for row in json_output["rows"]:
        log(format_output_row(row, max_length, types, breaches))

    if len(json_output["rows"]) != 0:
        log(format_output_divider(max_length))

    log(str_num_rows)


def handle_login(jaaql_url: str = None):
    load_file = False
    tenant = None
    username = None
    password = None
    if not jaaql_url:
        jaaql_url = input("Jaaql Url: ")
    elif jaaql_url.startswith("file "):
        return load_from_config_file(jaaql_url.split("file ")[1])

    if not load_file:
        tenant = input("Tenant: ").strip()
        username = input("Username: ").strip()
        password = getpass(prompt='Password: ', stream=None)

    return format_url(jaaql_url), tenant, username, password, None


was_go = False
fetched_query = ""
fetched_stdin = None
is_script = False
cur_mode = MODE__sql
connections = {}
connection_info = {}
connection_tokens = {}
current_connection = DEFAULT_CONNECTION
fetched_database = None
set_role = None
is_verbose = False


def print_error(err, do_exit=True):
    global is_script
    caller = getframeinfo(stack()[1][0])
    debug_message = " [%s:%d]" % (caller.filename, caller.lineno)
    if not is_script:
        debug_message = ""
    print(err + debug_message, file=sys.stderr)
    if do_exit:
        print(err + debug_message, file=sys.stdout)
        exit(1)


def log(msg):
    global is_verbose
    if is_verbose:
        print(str(msg))


def on_go():
    global is_script
    global fetched_query
    global was_go
    global current_connection
    global connections
    global connection_tokens
    global fetched_stdin
    global fetched_database
    global cur_mode

    if current_connection not in connections:
        print_error("You are missing a default connection. Please start your script with \\s connection_name if you wish to use only "
                    "named connections")

    was_go = False
    res = None

    if cur_mode == MODE__sql:
        the_endpoint = ENDPOINT__submit_file if is_script and "create database" not in fetched_query.lower() and "drop database" not in fetched_query.lower() else ENDPOINT__submit
        send_json = {"query": fetched_query}
        if fetched_database is not None:
            send_json["database"] = fetched_database
        if set_role is not None:
            send_json["role"] = set_role
        res = requests.post(jaaql_url + the_endpoint, json=send_json, headers=connection_tokens.get(current_connection))
    else:
        if '\g' in fetched_stdin:
            fetched_stdin = fetched_query
        commands = split_by_lines(fetched_stdin, 2)
        last_res = {}
        for command, x in zip(commands, range(len(commands))):
            if command.strip() == '\g' or len(command.strip()) == 0:
                continue
            command_split = split_by_lines(command)
            command_data = None
            if len(command_split) != 1:
                command_data = "\r\n".join(command_split[1:])

            if isinstance(last_res, dict) and command_data is not None:
                for key, val in last_res.items():
                    if "{{" + key + "}}" in command_data:
                        command_data = command_data.replace("{{" + key + "}}", val)
            url_part_one = command_split[0].split(" ")[1].split("?")[0]
            url_part_two = ""
            if len(command_split[0].split(" ")[1].split("?")) > 1:
                url_part_one = url_part_one + "?"
                url_part_two = "?".join(command_split[0].split(" ")[1].split("?")[1:])
                url_part_two = "&".join([urllib.parse.quote(part.split("=")[0]) + "=" + urllib.parse.quote(part.split("=")[1]) for part in url_part_two.split("&")])

            pass_json = None
            if command_data is not None and len(command_data.strip()) != 0:
                pass_json = json.loads(command_data)
            log(command_split[0].split(" ")[0] + " " + jaaql_url + url_part_one + url_part_two)
            if pass_json == {}:
                pass_json = None
            res = requests.request(command_split[0].split(" ")[0], jaaql_url + url_part_one + url_part_two, json=pass_json,
                                   headers=connection_tokens.get(current_connection))
            if pass_json is not None:
                log(json.dumps(pass_json))
            log(res.status_code)
            if res.status_code == 200:
                if x != len(commands) - 1:
                    log(json.dumps(res.json()))
                last_res = res.json()
            else:
                fetched_query = "\r\n".join(commands[x:])
                break

    if res.status_code == 401:
        was_go = True
        connection_tokens[current_connection] = None
        log("Refreshing oauth token")
    elif res.status_code == 200:
        if cur_mode == MODE__sql:
            format_query_output(res.json())
        fetched_query = ""
    else:
        print_error(res.text, is_script)
        fetched_query = ""


if __name__ == "__main__":
    args = sys.argv[1:]
    is_script = len([arg for arg in args if arg in ['-s', '--sql']]) != 0 or len([arg for arg in args if arg in ['-j', '--jaaql']]) != 0
    is_jaaql = len([arg for arg in args if arg in ['-j', '--jaaql']]) != 0
    is_verbose = len([arg for arg in args if arg in ['-v', '--verbose']]) != 0

    has_role = len([arg for arg in args if arg in ['-r', '--role']]) != 0
    if has_role:
        set_role = args[[arg_idx for arg, arg_idx in zip(args, range(len(args))) if arg in ['-r', '--role']][0] + 1]

    has_config = len([arg for arg in args if arg in ['-c', '--config']]) != 0
    if has_config:
        for arg, arg_idx in zip(args, range(len(args))):
            if arg not in ['-c', '--config']:
                continue
            if arg_idx == len(args) - 1:
                print_error("The config flag is the last argument. You need to supply a file")
            configuration_name = args[arg_idx + 1]
            candidate_file_name = None
            if arg_idx < len(args) - 1:
                candidate_file_name = args[arg_idx + 2]
            if candidate_file_name is None or candidate_file_name.startswith("<") or candidate_file_name.startswith("-"):
                candidate_file_name = configuration_name
                configuration_name = current_connection
            if configuration_name in connections:
                print_error("The configuration with name '" + configuration_name + "' already exists")
            connections[configuration_name] = candidate_file_name

    if is_jaaql:
        cur_mode = MODE__jaaql

    if not has_config and is_script:
        print_error("Must supply credentials file as argument in script mode")

    jaaql_url = None
    if has_config and connections.get(DEFAULT_CONNECTION):
        jaaql_url, tenant, username, password, fetched_database = load_from_config_file(connections[current_connection], current_connection)
    elif not is_script:
        print("Type jaaql url or \"file [config_file_location]\"")
        jaaql_url, tenant, username, password, fetched_database = handle_login(input("LOGIN>").strip())

    if jaaql_url is not None:
        log("Using url: " + jaaql_url)

    was_login = False
    was_eof = False

    file_lines = []

    while fetched_stdin != COMMAND__exit and not was_eof:
        do_go = False
        do_print = False

        if fetched_stdin is None:
            was_real_eof = True
            try:
                if len(file_lines) != 0:
                    fetched_stdin = file_lines[0]
                    file_lines = file_lines[1:]
                    if isinstance(fetched_stdin, EOFMarker):
                        was_real_eof = False
                        raise EOFError()
                else:
                    if is_script or is_jaaql:
                        fetched_stdin = input()
                    else:
                        fetched_stdin = input("JAAQL> ")
            except EOFError:
                if len(fetched_query.strip()) != 0:
                    print_error("Buffer was not empty when exiting. Please submit with /g before ending the script")
                was_eof = was_real_eof
                fetched_stdin = COMMAND__go

            if fetched_stdin is not None:
                fetched_stdin = fetched_stdin.strip()

        if connection_tokens.get(current_connection) is None and current_connection in connections:
            connection_tokens[current_connection] = fetch_oauth_token(jaaql_url, tenant, username, password,
                                                                      connection_tokens.get(current_connection) is None)
            was_login = connection_tokens.get(current_connection) is None

        if COMMAND__go in fetched_stdin:
            do_go = True
        if COMMAND__print in fetched_stdin:
            do_print = True

        if was_go:
            on_go()
        else:
            if fetched_stdin == COMMAND__login or was_login:
                oauth_token = None
                jaaql_url, username, password, fetched_database = handle_login()
                was_login = True
            elif fetched_stdin.startswith(COMMAND__mode + " "):
                if len(fetched_query.strip()) != 0:
                    print_error("Cannot switch modes while buffer is not empty. Please submit with \\g or clear with \\r", is_script)
                else:
                    fetched_mode = fetched_stdin.split(" ")[1]

                    if fetched_mode not in ALLOWED_MODES:
                        print_error("Mode '" + fetched_mode + "' not allowed. Allowed modes " + str(ALLOWED_MODES), is_script)
                    elif fetched_mode == cur_mode and not is_script:
                        print_error("Cannot switch to mode '" + cur_mode + "' as this is already the current processing mode", is_script)
                    else:
                        cur_mode = fetched_mode
            elif fetched_stdin.startswith(COMMAND__input + " "):
                if fetched_query != "":
                    print_error("Cannot load file until you have flushed your current buffer with \\g or reset with \\r", is_script)
                else:
                    load_file = fetched_stdin.split(" ")[1]
                    try:
                        file_lines = open(load_file, "r").readlines()
                        file_lines.append(EOFMarker())
                        log("Loaded file " + load_file)
                    except FileNotFoundError:
                        print_error("Cannot find file " + load_file, is_script)

            elif fetched_stdin == COMMAND__reset:
                log("Resetting buffer")
                fetched_query = ""
            elif fetched_stdin == COMMAND__help:
                print("JAAQL Monitor")
                print(COMMAND__print + ": Prints the command that has been input so far")
                print(COMMAND__go + ": Submits the command to jaaql")
                print(COMMAND__mode + " [mode]: Switches the mode. Accepts either 'sql' or 'jaaql'")
                print(COMMAND__exit + ": Exits the program")
                print(COMMAND__login + ": Logs in")
                print(COMMAND__switch + " [file]: Switches jaaql config files")
                print(COMMAND__reset + ": clears the input buffer")
                print(COMMAND__input + " [file]: Inputs a file")
            elif fetched_stdin.startswith(COMMAND__switch + " "):
                if len(fetched_query.strip()) != 0:
                    print_error("Cannot switch credentials while buffer is not empty. Please submit with \\g or reset with \\r", is_script)
                else:
                    possible_file_name = fetched_stdin.split(" ")[1]
                    connection = None
                    if possible_file_name in connections:
                        connection = possible_file_name
                        possible_file_name = connections[possible_file_name]

                    jaaql_url, tenant, username, password, fetched_database = load_from_config_file(possible_file_name, connection)
                    if connection is not None:
                        log("Switching to named connection '" + connection + '"')
                        current_connection = connection
                    else:
                        connection_info[current_connection] = jaaql_url, tenant, username, password, fetched_database
                        connection_tokens = {}
                        log("Now directing to " + username + "@" + jaaql_url)
            elif not do_go and not do_print and fetched_stdin != COMMAND__exit:
                fetched_query += fetched_stdin + "\n"
            elif do_go or do_print:
                was_command = False
                for i in range(len(fetched_stdin)):
                    the_char = fetched_stdin[i]
                    if the_char == "\\":
                        was_command = True
                    elif was_command and the_char == "p":
                        log(fetched_query)
                        was_command = False
                    elif was_command and the_char == "g":
                        on_go()
                        was_command = False
                    elif was_command:
                        fetched_query += "\\"
                        fetched_query += the_char
                        was_command = False
                    else:
                        fetched_query += the_char

        if fetched_stdin != COMMAND__exit and not was_go:
            fetched_stdin = None

    exit(0)
