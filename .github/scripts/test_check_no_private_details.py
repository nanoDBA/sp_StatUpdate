import sys
from pathlib import Path
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from check_no_private_details import ALLOWED_SQL_PASSWORD, scan_text


class PrivateDetailsScannerTests(unittest.TestCase):
    def test_private_addresses_and_paths_are_rejected(self):
        addresses = ((10, 1, 2, 3), (172, 20, 1, 2), (192, 168, 1, 4), (100, 70, 0, 1))
        text = "\n".join(".".join(map(str, address)) for address in addresses)
        text += "\nC:\\Users\\Someone\\file.sql\n/" + "mnt/data/file\n/" + "home/person/file\n"
        self.assertEqual(7, len(scan_text(text, "sample.txt")))

    def test_tailnet_email_tokens_and_private_keys_are_rejected(self):
        domain = "sample" + "." + "example" + "." + "ts.net"
        email = "a.person" + "@" + "example.org"
        private_key = "-----BEGIN " + "PRIVATE KEY" + "-----"
        token = "ghp_" + ("a" * 24)
        text = domain + "\n" + email + "\n" + private_key + "\n" + token
        categories = {category for _, category in scan_text(text, "sample.txt")}
        self.assertEqual({"tailnet domain", "email address", "private key", "GitHub or cloud token"}, categories)

    def test_beads_ids_are_rejected(self):
        examples = ("sp_StatUpdate-" + "abcd", "o2md." + "123")
        for example in examples:
            self.assertTrue(scan_text(example, "sample.md"))

    def test_literal_credentials_are_rejected(self):
        self.assertTrue(scan_text('SQL_PASSWORD: "' + "throwaway" + '"', "config.yml"))
        self.assertTrue(scan_text('Password="' + "do-not-publish" + '"', "connection.txt"))

    def test_environment_references_are_not_credentials(self):
        key = "MSSQL_SA_" + "PASSWORD"
        self.assertEqual([], scan_text(key + ': "${SPSTATUPDATE_TEST_SQL_PASSWORD}"', "compose.yml"))
        self.assertEqual([], scan_text("Password='$(AG_TEST_PASSWORD)'", "setup.sql"))

    def test_only_the_exact_workflow_test_password_is_allowed(self):
        key = "MSSQL_SA_" + "PASSWORD"
        line = key + ': "' + ALLOWED_SQL_PASSWORD + '"'
        self.assertEqual([], scan_text(line, ".github/workflows/sql-tests.yml"))
        self.assertTrue(scan_text(key + ': "different-value"', ".github/workflows/sql-tests.yml"))
        self.assertTrue(scan_text(line, "TESTING.md"))

    def test_loopback_is_allowed(self):
        self.assertEqual([], scan_text("127.0.0.1 localhost", "TESTING.md"))


if __name__ == "__main__":
    unittest.main()
