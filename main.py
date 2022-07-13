import sys
import requests
import json
from sys import exit
from getpass import getpass

ENDPOINT__oauth = "/oauth/token"
ENDPOINT__submit_file = "/submit-file"
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

MODE__sql = "sql"
MODE__jaaql = "jaaql"
ALLOWED_MODES = [MODE__sql, MODE__jaaql]

LINE_LENGTH_MAX = 115
ROWS_MAX = 25


class EOFMarker:
    pass


def print_error(err, do_exit=True):
    print(err, file=sys.stderr)
    if do_exit:
        print(err, file=sys.stdout)
        exit(1)


def fetch_oauth_token(jaaql_url, username, password, was_login):
    oauth_res = requests.post(jaaql_url + ENDPOINT__oauth, json={
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
    return url_input


def load_from_config_file(file_name):
    config = open(file_name, "r").read()
    config = split_by_lines(config)
    jaaql_url = format_url(config[0])
    username = config[1].strip()
    password = config[2].strip()

    print("Successfully loaded config")

    return jaaql_url, username, password


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

    print(format_output_divider(max_length))
    print(format_output_row(json_output["columns"], max_length, [str] * len(json_output["columns"]), [False] * len(max_length)))
    print(format_output_divider(max_length))

    if len(json_output["rows"]) > ROWS_MAX:
        json_output["rows"] = json_output["rows"][0:ROWS_MAX]
        json_output["rows"].append(["..." for _ in json_output["columns"]])

    for row in json_output["rows"]:
        print(format_output_row(row, max_length, types, breaches))

    if len(json_output["rows"]) != 0:
        print(format_output_divider(max_length))

    print(str_num_rows)


def handle_login(jaaql_url: str = None):
    load_file = False
    username = None
    password = None
    if not jaaql_url:
        jaaql_url = input("Jaaql Url: ")
    elif jaaql_url.startswith("file "):
        return load_from_config_file(jaaql_url.split("file ")[1])

    if not load_file:
        username = input("Username: ").strip()
        password = getpass(prompt='Password: ', stream=None)

    return format_url(jaaql_url), username, password


was_go = False
fetched_query = ""
oauth_token = None
fetched_stdin = None


def on_go():
    global fetched_query
    global oauth_token
    global was_go
    global fetched_stdin

    was_go = False
    res = None

    if cur_mode == MODE__sql:
        the_endpoint = ENDPOINT__submit_file if is_script else ENDPOINT__submit
        res = requests.post(jaaql_url + the_endpoint, json={"query": fetched_query}, headers=oauth_token)
    else:
        commands = split_by_lines(fetched_stdin, 2)
        for command, x in zip(commands, range(len(commands))):
            command_split = split_by_lines(command)
            command_data = None
            if len(command_split) == 1:
                command_data = None
            else:
                command_data = command_split[1:]
            res = requests.request(command_split[0].split(" ")[0], jaaql_url + command_split[1].split(" ")[1], json=command_data,
                                   headers=oauth_token)
            if res.status_code == 200:
                if x != len(commands) - 1:
                    print(json.dumps(res.json()))
            else:
                fetched_query = commands[x:].join("\n\n")
                break

    if res.status_code == 401:
        was_go = True
        oauth_token = None
        print("Refreshing oauth token")
    elif res.status_code == 200:
        format_query_output(res.json())
        fetched_query = ""
    else:
        print_error(res.text, is_script)
        fetched_query = ""


if __name__ == "__main__":
    args = sys.argv[1:]
    is_script = len([arg for arg in args if arg in ['-s', '--script']]) != 0

    if len(sys.argv) < 3 and is_script:
        print_error("Must supply credentials file as argument in script mode")

    if len(sys.argv) < 2:
        print("Type jaaql url or file [config_file_location]")
        jaaql_url, username, password = handle_login(input("LOGIN>").strip())
    else:
        jaaql_url, username, password = load_from_config_file([arg for arg in args if arg not in ['-s', '--script']][0])

    cur_mode = MODE__sql
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
                    if is_script:
                        fetched_stdin = input()
                    else:
                        fetched_stdin = input("JAAQL>")
            except EOFError:
                was_eof = was_real_eof
                fetched_stdin = COMMAND__go

            fetched_stdin = fetched_stdin.strip()

        if oauth_token is None:
            oauth_token = fetch_oauth_token(jaaql_url, username, password, was_login)
            was_login = oauth_token is None

        if COMMAND__go in fetched_stdin:
            do_go = True
        if COMMAND__print in fetched_stdin:
            do_print = True

        if was_go:
            on_go()
        else:
            if fetched_stdin == COMMAND__login or was_login:
                oauth_token = None
                jaaql_url, username, password = handle_login()
                was_login = True
            elif fetched_stdin.startswith(COMMAND__mode + " "):
                if len(fetched_query) != 0:
                    print_error("Cannot switch modes while query has not been submitted. Please submit with \g", is_script)
                else:
                    fetched_mode = fetched_stdin.split(" ")[1]

                    if fetched_mode not in ALLOWED_MODES:
                        print_error("Mode '" + fetched_mode + "' not allowed. Allowed modes " + str(ALLOWED_MODES), is_script)
                    elif fetched_mode == cur_mode:
                        print_error("Cannot switch to mode '" + cur_mode + "' as this is already the current processing mode", is_script)
                    else:
                        cur_mode = fetched_mode
            elif fetched_stdin.startswith(COMMAND__input + " "):
                if fetched_query != "":
                    print_error("Cannot load file until you have flushed your current buffer with \g", is_script)
                else:
                    load_file = fetched_stdin.split(" ")[1]
                    try:
                        file_lines = open(load_file, "r").readlines()
                        file_lines.append(EOFMarker())
                        print("Loaded file " + load_file)
                    except FileNotFoundError:
                        print_error("Cannot find file " + load_file, is_script)

            elif fetched_stdin == COMMAND__reset:
                print("Resetting buffer")
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
                oauth_token = None
                jaaql_url, username, password = load_from_config_file(fetched_stdin.split(" ")[1])
                print("Now directing to " + username + "@" + jaaql_url)
            elif not do_go and not do_print and fetched_stdin != COMMAND__exit:
                fetched_query += fetched_stdin + "\n"
            elif do_go or do_print:
                was_command = False
                for i in range(len(fetched_stdin)):
                    the_char = fetched_stdin[i]
                    if the_char == "\\":
                        was_command = True
                    elif was_command and the_char == "p":
                        print(fetched_query)
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
