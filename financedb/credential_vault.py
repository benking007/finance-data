"""Encrypt optional operator credentials with a private key on the service host."""

import os
from pathlib import Path
import stat
import tempfile

from cryptography.fernet import Fernet, InvalidToken


class CredentialVault:
    def __init__(self, sqlite_path):
        self.path = Path(f"{Path(sqlite_path).resolve()}.credentials.key") if sqlite_path not in {None, "", ":memory:"} else None

    def _key(self, *, create):
        if self.path is None:
            raise OSError("A file-backed local configuration database is required")
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
        try:
            fd = os.open(self.path, flags)
        except FileNotFoundError:
            if not create:
                raise
            # Publish a completed private key so concurrent service startups
            # cannot see a partial file or replace an established key.
            fd, temporary = tempfile.mkstemp(prefix=f".{self.path.name}.", dir=self.path.parent)
            try:
                with os.fdopen(fd, "wb") as writer:
                    writer.write(Fernet.generate_key())
                    writer.flush()
                    os.fsync(writer.fileno())
                try:
                    os.link(temporary, self.path)
                except FileExistsError:
                    pass
            finally:
                os.unlink(temporary)
            fd = os.open(self.path, flags)
        with os.fdopen(fd, "rb") as reader:
            info = os.fstat(reader.fileno())
            if (not stat.S_ISREG(info.st_mode) or stat.S_IMODE(info.st_mode) & 0o077
                    or (hasattr(os, "geteuid") and info.st_uid != os.geteuid())):
                raise PermissionError("The credential key must be a private file owned by the service user")
            return Fernet(reader.read(128))

    def seal(self, context, secret):
        try:
            return self._key(create=True).encrypt(f"{context}\x00{secret}".encode()).decode("ascii")
        except (OSError, ValueError):
            raise RuntimeError("无法加密保存凭证，请检查配置库目录与凭证密钥文件的权限") from None

    def open(self, context, encrypted):
        if not isinstance(encrypted, str):
            return None
        try:
            clear = self._key(create=False).decrypt(encrypted.encode("ascii")).decode()
        except (OSError, ValueError, InvalidToken):
            return None
        purpose, _, secret = clear.partition("\x00")
        return secret if purpose == context else None
