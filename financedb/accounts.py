"""Persistent website accounts, sessions, API keys and daily quotas."""

from contextlib import contextmanager
from datetime import datetime, time, timedelta
from hashlib import pbkdf2_hmac, sha256
from hmac import compare_digest
import math
import os
from pathlib import Path
import re
from secrets import token_bytes, token_urlsafe
import sqlite3
from zoneinfo import ZoneInfo


DEFAULT_ACCOUNTS_PATH = "data/accounts.sqlite3"
DEFAULT_DAILY_LIMIT = 3000
MAX_DAILY_LIMIT = 2_147_483_647
MAX_USER_ID = 9_223_372_036_854_775_807
SESSION_SECONDS = 8 * 60 * 60
PASSWORD_ITERATIONS = 600_000
SHANGHAI = ZoneInfo("Asia/Shanghai")
USERNAME = re.compile(r"[a-z0-9][a-z0-9._-]{2,31}", re.ASCII)
SESSION_TOKEN = re.compile(r"[A-Za-z0-9_-]{43}")
API_TOKEN = re.compile(r"fdb_[A-Za-z0-9_-]{43}")


class AccountConflict(ValueError):
    pass


class InvalidCredentials(ValueError):
    pass


class AccountForbidden(PermissionError):
    pass


class AccountNotFound(LookupError):
    pass


class QuotaExceeded(RuntimeError):
    def __init__(self, retry_after):
        super().__init__("今日查询额度已用完")
        self.retry_after = retry_after


def _now():
    return datetime.now(SHANGHAI)


def _username(value):
    normalized = value.strip().lower() if isinstance(value, str) and value.isascii() else ""
    if not USERNAME.fullmatch(normalized):
        raise ValueError("用户名须为 3 至 32 个字符，以字母或数字开头，可包含英文句点、下划线和短横线")
    return normalized


def _password_bytes(password):
    if not isinstance(password, str) or not 10 <= len(password) <= 256:
        raise ValueError("密码须为 10 至 256 个字符")
    try:
        return password.encode("utf-8")
    except UnicodeError:
        raise ValueError("密码包含无法编码的字符") from None


def _password_hash(password, salt):
    return pbkdf2_hmac("sha256", password, salt, PASSWORD_ITERATIONS)


def _token_hash(token):
    return sha256(token.encode("ascii")).digest()


def _validate_user_id(user_id):
    if type(user_id) is not int or not 1 <= user_id <= MAX_USER_ID:
        raise AccountNotFound("账号不存在")


def _account(row, today, *, used=None):
    if used is None:
        used = row["daily_used"] if row["quota_date"] == today else 0
    suffix = row["api_key_suffix"]
    return {
        "id": row["id"], "username": row["username"], "is_admin": bool(row["is_admin"]),
        "daily_limit": row["daily_limit"], "daily_used": used, "used_today": used, "quota_date": today,
        "remaining_today": max(0, row["daily_limit"] - used), "has_api_key": suffix is not None,
        "api_key_mask": f"fdb_••••{suffix}" if suffix is not None else None,
    }


class Accounts:
    def __init__(self, path=None):
        self.path = Path(path or os.getenv("FINANCEDB_ACCOUNTS_PATH") or DEFAULT_ACCOUNTS_PATH).expanduser().resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError:
            pass
        else:
            os.close(fd)
        with self._connection(write=True) as conn:
            tables = {row[0] for row in conn.execute(
                "SELECT name FROM sqlite_schema WHERE type = 'table' AND name NOT GLOB 'sqlite_*'"
            )}
            if tables - {"account_users", "account_sessions"}:
                raise ValueError("账号库必须使用独立的 SQLite 文件")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS account_users (
                    id INTEGER PRIMARY KEY,
                    username TEXT NOT NULL UNIQUE,
                    password_hash BLOB NOT NULL,
                    password_salt BLOB NOT NULL,
                    is_admin INTEGER NOT NULL CHECK (is_admin IN (0, 1)),
                    daily_limit INTEGER NOT NULL CHECK (daily_limit >= 0),
                    quota_date TEXT NOT NULL DEFAULT '',
                    daily_used INTEGER NOT NULL DEFAULT 0,
                    api_key_hash BLOB UNIQUE,
                    api_key_suffix TEXT
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS account_sessions (
                    token_hash BLOB PRIMARY KEY NOT NULL,
                    user_id INTEGER NOT NULL REFERENCES account_users (id) ON DELETE CASCADE,
                    expires_at INTEGER NOT NULL
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS account_sessions_expiry ON account_sessions (expires_at)")

    @contextmanager
    def _connection(self, *, write=False):
        conn = sqlite3.connect(self.path, timeout=15, isolation_level=None)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA foreign_keys = ON")
            if write:
                conn.execute("BEGIN IMMEDIATE")
            yield conn
            if write:
                conn.commit()
        finally:
            conn.close()

    @staticmethod
    def _find_user(conn, user_id):
        _validate_user_id(user_id)
        row = conn.execute("SELECT * FROM account_users WHERE id = ?", (user_id,)).fetchone()
        if row is None:
            raise AccountNotFound("账号不存在")
        return row

    def _require_admin(self, conn, admin_id):
        if not self._find_user(conn, admin_id)["is_admin"]:
            raise AccountForbidden("仅管理员可以管理账号额度")

    @staticmethod
    def _issue_session(conn, user_id, now):
        token = token_urlsafe(32)
        issued_at = int(now.timestamp())
        conn.execute("DELETE FROM account_sessions WHERE expires_at <= ?", (issued_at,))
        conn.execute("INSERT INTO account_sessions (token_hash, user_id, expires_at) VALUES (?, ?, ?)",
                     (_token_hash(token), user_id, issued_at + SESSION_SECONDS))
        return token

    def register(self, username, password):
        username = _username(username)
        encoded = _password_bytes(password)
        salt = token_bytes(16)
        proof = _password_hash(encoded, salt)
        with self._connection(write=True) as conn:
            if conn.execute("SELECT 1 FROM account_users WHERE username = ?", (username,)).fetchone():
                raise AccountConflict("用户名已经注册")
            first = conn.execute("SELECT 1 FROM account_users LIMIT 1").fetchone() is None
            user_id = conn.execute("""
                INSERT INTO account_users (username, password_hash, password_salt, is_admin, daily_limit)
                VALUES (?, ?, ?, ?, ?)
            """, (username, proof, salt, first, DEFAULT_DAILY_LIMIT)).lastrowid
            now = _now()
            token = self._issue_session(conn, user_id, now)
            return _account(self._find_user(conn, user_id), now.date().isoformat()), token

    def login(self, username, password):
        try:
            username = _username(username)
            encoded = _password_bytes(password)
        except ValueError:
            raise InvalidCredentials("用户名或密码不正确") from None
        with self._connection() as conn:
            row = conn.execute("SELECT * FROM account_users WHERE username = ?", (username,)).fetchone()
        salt, expected = (row["password_salt"], row["password_hash"]) if row else (bytes(16), bytes(32))
        supplied = _password_hash(encoded, salt)
        if not compare_digest(supplied, expected) or row is None:
            raise InvalidCredentials("用户名或密码不正确")
        with self._connection(write=True) as conn:
            now = _now()
            token = self._issue_session(conn, row["id"], now)
            return _account(self._find_user(conn, row["id"]), now.date().isoformat()), token

    def session_user(self, token, *, consume=False):
        if not isinstance(token, str) or not SESSION_TOKEN.fullmatch(token):
            return None
        with self._connection(write=consume) as conn:
            now = _now()
            row = conn.execute("""
                SELECT account_users.* FROM account_sessions
                JOIN account_users ON account_users.id = account_sessions.user_id
                WHERE account_sessions.token_hash = ? AND account_sessions.expires_at > ?
            """, (_token_hash(token), int(now.timestamp()))).fetchone()
            if row is None:
                return None
            return self._consume(conn, row, now) if consume else _account(row, now.date().isoformat())

    def logout(self, token):
        if isinstance(token, str) and SESSION_TOKEN.fullmatch(token):
            with self._connection(write=True) as conn:
                conn.execute("DELETE FROM account_sessions WHERE token_hash = ?", (_token_hash(token),))

    def generate_api_key(self, user_id):
        _validate_user_id(user_id)
        key = "fdb_" + token_urlsafe(32)
        with self._connection(write=True) as conn:
            result = conn.execute("UPDATE account_users SET api_key_hash = ?, api_key_suffix = ? WHERE id = ?",
                                  (_token_hash(key), key[-4:], user_id))
            if not result.rowcount:
                raise AccountNotFound("账号不存在")
            return key, _account(self._find_user(conn, user_id), _now().date().isoformat())

    def api_user(self, key, *, consume=False):
        if not isinstance(key, str) or not API_TOKEN.fullmatch(key):
            return None
        with self._connection(write=consume) as conn:
            row = conn.execute("SELECT * FROM account_users WHERE api_key_hash = ?", (_token_hash(key),)).fetchone()
            if row is None:
                return None
            now = _now()
            return self._consume(conn, row, now) if consume else _account(row, now.date().isoformat())

    @staticmethod
    def _consume(conn, row, now):
        today = now.date().isoformat()
        used = row["daily_used"] if row["quota_date"] == today else 0
        if used >= row["daily_limit"]:
            tomorrow = datetime.combine(now.date() + timedelta(days=1), time.min, SHANGHAI)
            raise QuotaExceeded(max(1, math.ceil((tomorrow - now).total_seconds())))
        used += 1
        conn.execute("UPDATE account_users SET quota_date = ?, daily_used = ? WHERE id = ?",
                     (today, used, row["id"]))
        return _account(row, today, used=used)

    def list_users(self, admin_id):
        today = _now().date().isoformat()
        with self._connection() as conn:
            self._require_admin(conn, admin_id)
            return [_account(row, today) for row in conn.execute("SELECT * FROM account_users ORDER BY id")]

    def set_daily_limit(self, admin_id, user_id, daily_limit):
        if type(daily_limit) is not int or not 0 <= daily_limit <= MAX_DAILY_LIMIT:
            raise ValueError(f"额度须为 0 至 {MAX_DAILY_LIMIT} 的整数")
        with self._connection(write=True) as conn:
            self._require_admin(conn, admin_id)
            _validate_user_id(user_id)
            if not conn.execute("UPDATE account_users SET daily_limit = ? WHERE id = ?", (daily_limit, user_id)).rowcount:
                raise AccountNotFound("账号不存在")
            return _account(self._find_user(conn, user_id), _now().date().isoformat())
