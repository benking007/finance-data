from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from financedb.accounts import (
    AccountConflict, AccountForbidden, Accounts, InvalidCredentials, QuotaExceeded, SESSION_SECONDS, SHANGHAI,
)


PASSWORD = "fixture-strong-password-for-accounts"


class AccountsTest(unittest.TestCase):
    def setUp(self):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / "accounts.sqlite3"
        self.accounts = Accounts(self.path)

    def test_password_and_sessions_are_persistent_and_hashed(self):
        now = datetime(2026, 11, 25, 10, tzinfo=SHANGHAI)
        with patch("financedb.accounts._now", return_value=now):
            first, token = self.accounts.register("First.Admin", PASSWORD)
            second, _ = self.accounts.register("Reader-A", PASSWORD)
            self.assertTrue(first["is_admin"])
            self.assertFalse(second["is_admin"])
            self.assertEqual(second["daily_limit"], 3000)
            with self.assertRaises(AccountConflict):
                self.accounts.register("FIRST.ADMIN", PASSWORD)
            with self.assertRaises(InvalidCredentials):
                self.accounts.login("first.admin", "10-or-more-but-wrong")
            fresh = Accounts(self.path)
            self.assertEqual(fresh.session_user(token), first)
            user, other_token = fresh.login("FIRST.Admin", PASSWORD)
            self.assertEqual(user, first)
            self.assertNotEqual(other_token, token)
            fresh.logout(token)
            self.assertIsNone(self.accounts.session_user(token))
            with sqlite3.connect(self.path) as conn:
                password_proofs = conn.execute("SELECT password_hash, password_salt FROM account_users ORDER BY id").fetchall()
                stored_sessions = [row[0] for row in conn.execute("SELECT token_hash FROM account_sessions")]
            self.assertNotEqual(password_proofs[0], password_proofs[1])
            self.assertNotIn(PASSWORD.encode(), self.path.read_bytes())
            self.assertNotIn(other_token.encode(), stored_sessions)
        with patch("financedb.accounts._now", return_value=now + timedelta(seconds=SESSION_SECONDS)):
            self.assertIsNone(self.accounts.session_user(other_token))

    def test_key_rotation_and_shared_quota_reset_at_beijing_midnight(self):
        now = datetime(2026, 11, 25, 23, 59, 58, tzinfo=SHANGHAI)
        with patch("financedb.accounts._now", return_value=now):
            admin, _ = self.accounts.register("admin", PASSWORD)
            reader, token = self.accounts.register("reader", PASSWORD)
            with self.assertRaises(AccountForbidden):
                self.accounts.list_users(reader["id"])
            with self.assertRaises(AccountForbidden):
                self.accounts.set_daily_limit(reader["id"], admin["id"], 2)
            self.accounts.set_daily_limit(admin["id"], reader["id"], 2)
            key, user = self.accounts.generate_api_key(reader["id"])
            self.assertTrue(user["has_api_key"])
            self.assertNotIn(key, str(user))
            self.assertNotIn(key.encode(), self.path.read_bytes())
            self.assertEqual(self.accounts.api_user(key)["used_today"], 0)
            self.assertEqual(self.accounts.api_user(key, consume=True)["used_today"], 1)
            other_key, user = self.accounts.generate_api_key(reader["id"])
            self.assertIsNone(self.accounts.api_user(key, consume=True))
            self.assertEqual(user["used_today"], 1)
            self.assertEqual(self.accounts.session_user(token, consume=True)["remaining_today"], 0)
            with self.assertRaises(QuotaExceeded) as over_limit:
                self.accounts.api_user(other_key, consume=True)
            self.assertEqual(over_limit.exception.retry_after, 2)

        with patch("financedb.accounts._now", return_value=now + timedelta(seconds=2)):
            self.assertEqual(self.accounts.api_user(other_key)["quota_date"], "2026-11-26")
            self.assertEqual(self.accounts.session_user(token)["used_today"], 0)
            self.assertEqual(self.accounts.api_user(other_key, consume=True)["remaining_today"], 1)
            self.accounts.set_daily_limit(admin["id"], reader["id"], 0)
            with self.assertRaises(QuotaExceeded):
                self.accounts.session_user(token, consume=True)
            summary = self.accounts.list_users(admin["id"])[1]
            self.assertEqual(summary["daily_limit"], 0)
            self.assertEqual(summary["used_today"], 1)

    def test_concurrent_registration_and_quota_consumption(self):
        with ThreadPoolExecutor(max_workers=8) as pool:
            registered = list(pool.map(lambda index: Accounts(self.path).register(f"user{index}", PASSWORD), range(5)))
            self.assertEqual(sum(user["is_admin"] for user, _ in registered), 1)
            admin = next(user for user, _ in registered if user["is_admin"])
            member, session = next((user, token) for user, token in registered if not user["is_admin"])
            self.accounts.set_daily_limit(admin["id"], member["id"], 5)
            key, _ = self.accounts.generate_api_key(member["id"])

            def request(index):
                try:
                    if index % 2:
                        self.accounts.api_user(key, consume=True)
                    else:
                        self.accounts.session_user(session, consume=True)
                except QuotaExceeded:
                    return False
                return True

            self.assertEqual(sum(pool.map(request, range(18))), 5)
        self.assertEqual(self.accounts.session_user(session)["used_today"], 5)

    def test_account_store_rejects_business_database(self):
        other = self.path.parent / "business.sqlite3"
        with sqlite3.connect(other) as conn:
            conn.execute("CREATE TABLE schema_versions (version INTEGER PRIMARY KEY)")
        with self.assertRaisesRegex(ValueError, "独立的 SQLite 文件"):
            Accounts(other)
