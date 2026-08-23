"""ResilientIMAP: an IMAP4_SSL wrapper that reconnects on transient failures."""
import imaplib
import logging
import socket
import ssl
import time

from imap_common import create_ssl_context

CONNECTION_TIMEOUT = 60

log = logging.getLogger(__name__)


class ResilientIMAP:
    def __init__(self, host, user, password, timeout=CONNECTION_TIMEOUT, retries=3):
        self.host = host
        self.user = user
        self.password = password
        self.timeout = timeout
        self.retries = retries
        self.mail = None
        self.current_folder = None
        self.readonly = False
        self._connect()

    def _connect(self):
        if self.mail:
            try:
                self.mail.logout()
            except Exception:
                pass
        self.mail = imaplib.IMAP4_SSL(self.host, ssl_context=create_ssl_context(), timeout=self.timeout)
        self.mail.login(self.user, self.password)
        log.info("Connected to %s as %s", self.host, self.user)
        if self.current_folder is not None:
            self.mail.select(self.current_folder, readonly=self.readonly)

    def select(self, folder, readonly=False):
        self.current_folder = folder
        self.readonly = readonly
        # Strip any stray quotes the user may have typed before re-quoting.
        safe = folder.strip('"')
        return self.mail.select(f'"{safe}"', readonly=readonly)

    def _retry(self, op_name, *args, **kwargs):
        last_exc = RuntimeError(f"Operation '{op_name}' failed after retries")
        for attempt in range(self.retries):
            try:
                func = getattr(self.mail, op_name)
                return func(*args, **kwargs)
            except (imaplib.IMAP4.abort, socket.error, EOFError, ssl.SSLError) as exc:
                last_exc = exc
                log.warning("%s failed (%s); retry %d/%d", op_name, exc, attempt + 1, self.retries)
                if attempt < self.retries - 1:
                    time.sleep(2 * (attempt + 1))
                    try:
                        self._connect()
                    except Exception as reconnect_exc:
                        last_exc = reconnect_exc
        raise last_exc

    def uid(self, *args, **kwargs):
        return self._retry("uid", *args, **kwargs)

    def list(self, *args, **kwargs):
        return self._retry("list", *args, **kwargs)

    def expunge(self):
        return self._retry("expunge")

    def capabilities(self):
        return self.mail.capabilities

    def logout(self):
        try:
            self.mail.close()
        except Exception:
            pass
        try:
            self.mail.logout()
        except Exception:
            pass
