import io
import unittest
from base64 import urlsafe_b64encode as b64e
from contextlib import redirect_stderr
from unittest import mock

from monitor import main

USERNAME = "superjaaql"


class FakeResponse:
    status_code = 200
    text = "{}"

    def json(self):
        return {"columns": ["x"], "rows": [[1]]}


def run_script(script):
    """Feeds the script through the monitor's line handling and returns every request it would have sent,
    with the connection's username and the JSON body at that moment."""
    sent = []

    def request_handler(state, method, endpoint, send_json=None, **kwargs):
        sent.append({"endpoint": endpoint, "username": state.get_current_connection().username, "json": send_json})
        return FakeResponse()

    encoded = ":".join(b64e(value.encode()).decode() for value in ["http://127.0.0.1", USERNAME, "pa55word"])
    with mock.patch.object(main.State, "request_handler", autospec=True, side_effect=request_handler), redirect_stderr(io.StringIO()):
        main.initialise_from_args(["-s", "--encoded-config", main.DEFAULT_CONNECTION, encoded],
                                  file_name="test.jaaql", file_content=script, do_exit=False)
    return sent


class CteIsSql(unittest.TestCase):
    def test_cte_named_users_on_an_empty_buffer(self):
        sent = run_script("WITH users AS (SELECT 1 AS x) SELECT x FROM users;\n\\g\n")
        self.assertEqual(len(sent), 1)
        self.assertEqual(sent[0]["json"]["query"], "WITH users AS (SELECT 1 AS x) SELECT x FROM users;\n")
        self.assertEqual(sent[0]["username"], USERNAME)

    def test_cte_named_users_after_other_sql(self):
        sent = run_script("SELECT 1;\nWITH users AS (SELECT 1 AS x) SELECT x FROM users;\n\\g\n")
        self.assertEqual(sent[0]["json"]["query"], "SELECT 1;\nWITH users AS (SELECT 1 AS x) SELECT x FROM users;\n")
        self.assertEqual(sent[0]["username"], USERNAME)

    def test_lower_case_cte_named_user_rows(self):
        sent = run_script("with user_rows as (\n    SELECT 1 AS x\n)\nSELECT x FROM user_rows;\n\\g\n")
        self.assertEqual(sent[0]["json"]["query"], "with user_rows as (\n    SELECT 1 AS x\n)\nSELECT x FROM user_rows;\n")
        self.assertEqual(sent[0]["username"], USERNAME)

    def test_cte_named_user_is_left_to_postgres(self):
        sent = run_script("WITH user AS (SELECT 1 AS x) SELECT x FROM user;\n\\g\n")
        self.assertEqual(sent[0]["json"]["query"], "WITH user AS (SELECT 1 AS x) SELECT x FROM user;\n")
        self.assertEqual(sent[0]["username"], USERNAME)

    def test_cte_inside_a_function_body(self):
        script = ("CREATE FUNCTION f() RETURNS int AS $$\nWITH users AS (SELECT 1 AS x)\nSELECT x FROM users;\n"
                  "$$ LANGUAGE sql;\n\\g\n")
        sent = run_script(script)
        self.assertEqual(sent[0]["json"]["query"], script[:-len("\\g\n")])


class WithUser(unittest.TestCase):
    def test_switches_the_user(self):
        sent = run_script("WITH USER Graham@Bolton.nl\nSELECT 1;\n\\g\n")
        self.assertEqual(sent[0]["username"], "graham@bolton.nl")
        self.assertEqual(sent[0]["json"]["query"], "SELECT 1;\n")

    def test_lower_case_and_trailing_whitespace(self):
        sent = run_script("with user someone  \nSELECT 1;\n\\g\n")
        self.assertEqual(sent[0]["username"], "someone")

    def test_refused_after_sql_without_a_go(self):
        with self.assertRaisesRegex(main.JAAQLMonitorException, "but buffer was non empty"):
            run_script("SELECT 1;\nWITH USER someone\n")

    def test_quoted_user_refused(self):
        with self.assertRaisesRegex(main.JAAQLMonitorException, "Please do not quote the user!"):
            run_script("WITH USER 'someone'\nSELECT 1;\n\\g\n")


class WithParameters(unittest.TestCase):
    def test_single_line(self):
        sent = run_script("WITH PARAMETERS {\"n\": 2}\nSELECT :n AS x;\n\\g\n")
        self.assertEqual(sent[0]["json"]["parameters"], {"n": 2})
        self.assertEqual(sent[0]["json"]["query"], "SELECT :n AS x;\n")

    def test_multi_line_closed_with_user(self):
        sent = run_script("WITH PARAMETERS {\n    \"n\": 2\n} WITH USER someone\nSELECT :n AS x;\n\\g\n")
        self.assertEqual(sent[0]["json"]["parameters"], {"n": 2})
        self.assertEqual(sent[0]["username"], "someone")

    def test_lower_case(self):
        sent = run_script("with parameters {\"n\": 2}\nSELECT :n AS x;\n\\g\n")
        self.assertEqual(sent[0]["json"]["parameters"], {"n": 2})

    def test_followed_by_a_cte_named_users(self):
        sent = run_script("WITH PARAMETERS {\"n\": 2}\nWITH users AS (SELECT :n AS x) SELECT x FROM users;\n\\g\n")
        self.assertEqual(sent[0]["json"]["parameters"], {"n": 2})
        self.assertEqual(sent[0]["json"]["query"], "WITH users AS (SELECT :n AS x) SELECT x FROM users;\n")
        self.assertEqual(sent[0]["username"], USERNAME)


class ConnectToDatabase(unittest.TestCase):
    def test_for_extension_configuration_is_autocommit(self):
        sent = run_script("\\connect to database database for extension configuration\n"
                          "CREATE EXTENSION IF NOT EXISTS \"unaccent\";\nCREATE EXTENSION IF NOT EXISTS \"pg_trgm\";\n\\g\n")
        self.assertEqual(sent[0]["json"]["database"], "database")
        self.assertIs(sent[0]["json"].get("autocommit"), True)

    def test_for_createdb_is_autocommit(self):
        sent = run_script("\\connect to database postgres for createdb\nCREATE DATABASE database;\\g\n")
        self.assertEqual(sent[0]["json"]["database"], "postgres")
        self.assertIs(sent[0]["json"].get("autocommit"), True)

    def test_plain_connect_is_transactional(self):
        sent = run_script("\\connect to database jaaql\nSELECT 1;\n\\g\n")
        self.assertEqual(sent[0]["json"]["database"], "jaaql")
        self.assertNotIn("autocommit", sent[0]["json"])


if __name__ == "__main__":
    unittest.main()
