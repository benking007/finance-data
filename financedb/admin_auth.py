"""In-memory sessions for authenticated local operations."""

from collections import deque
from hashlib import sha256
from math import ceil
from secrets import compare_digest, token_urlsafe
from threading import Lock
from time import monotonic

from fastapi import HTTPException


COOKIE_NAME = "financedb_operator_session"
SESSION_SECONDS = 8 * 60 * 60
FAILED_LOGIN_LIMIT = 5
LOGIN_WINDOW_SECONDS = 60


class OperatorSessions:
    def __init__(self, password: str):
        if len(password) > 512:
            raise ValueError("FINANCEDB_ADMIN_PASSWORD 最多 512 个字符")
        self._password_digest = sha256(password.encode(errors="surrogatepass")).digest()
        self._sessions: dict[str, float] = {}
        self._failures: deque[float] = deque()
        self._lock = Lock()

    def authenticated(self, token: str) -> bool:
        with self._lock:
            now = monotonic()
            expires = self._sessions.get(token, 0)
            if expires > now:
                return True
            self._sessions.pop(token, None)
            return False

    def login(self, password: str, previous_token: str) -> str:
        password_digest = sha256(password.encode(errors="surrogatepass")).digest()
        with self._lock:
            now = monotonic()
            while self._failures and self._failures[0] <= now - LOGIN_WINDOW_SECONDS:
                self._failures.popleft()
            if len(self._failures) >= FAILED_LOGIN_LIMIT:
                retry_after = ceil(self._failures[0] + LOGIN_WINDOW_SECONDS - now)
                raise HTTPException(429, "登录尝试过多，请稍后再试", headers={"Retry-After": str(retry_after)})
            if not compare_digest(password_digest, self._password_digest):
                self._failures.append(now)
                raise HTTPException(401, "管理员密码不正确")

            token = token_urlsafe(32)
            self._failures.clear()
            self._sessions = {key: expiry for key, expiry in self._sessions.items() if expiry > now}
            self._sessions.pop(previous_token, None)
            self._sessions[token] = now + SESSION_SECONDS
            return token

    def logout(self, token: str) -> None:
        with self._lock:
            self._sessions.pop(token, None)
