from __future__ import annotations

import base64
import email
import imaplib
import socket
import ssl
import sys
from email.message import Message
from typing import Iterator, Literal, Optional, Tuple

def _enable_tcp_keepalive(mail: imaplib.IMAP4) -> None:
    """Включить TCP keepalive на сокете IMAP: помогает обнаруживать «тихие»
    обрывы NAT/firewall’ом раньше, чем IDLE-сессия молча провисит часы."""
    sock = getattr(mail, "sock", None)
    if sock is None:
        return
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    except OSError:
        return

    for opt_name, val in (
        ("TCP_KEEPIDLE", 60),
        ("TCP_KEEPINTVL", 30),
        ("TCP_KEEPCNT", 4),
        ("TCP_KEEPALIVE", 60),
    ):
        opt = getattr(socket, opt_name, None)
        if opt is None:
            continue
        try:
            sock.setsockopt(socket.IPPROTO_TCP, opt, val)
        except OSError:
            pass

IdleWake = Literal["exists", "timeout", "unsupported"]

def fetch_uids_after(
    mail: imaplib.IMAP4, last_uid: int, limit: Optional[int]
) -> list[bytes]:
    if last_uid <= 0:
        status, data = mail.uid("SEARCH", None, "ALL")
    else:
        status, data = mail.uid("SEARCH", None, "UID", f"{last_uid + 1}:*")
    if status != "OK" or not data or not data[0]:
        return []
    uids = data[0].split()
    if limit is not None and len(uids) > limit:
        uids = uids[-limit:]
    return uids

def encode_imap_modified_utf7(s: str) -> str:
    res: list[str] = []
    buf: list[str] = []

    def flush() -> None:
        if not buf:
            return
        u16 = "".join(buf).encode("utf-16-be")
        b64 = base64.b64encode(u16).decode("ascii").rstrip("=").replace("/", ",")
        res.append("&" + b64 + "-")
        buf.clear()

    for c in s:
        o = ord(c)
        if c == "&":
            flush()
            res.append("&-")
        elif 0x20 <= o <= 0x7E:
            flush()
            res.append(c)
        else:
            buf.append(c)
    flush()
    return "".join(res)

def quote_imap_mailbox(name: str) -> str:
    inner = name.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{inner}"'

def try_enable_imap_utf8(mail: imaplib.IMAP4) -> None:
    caps = getattr(mail, "capabilities", None)
    if not caps:
        return
    if "ENABLE" not in caps or "UTF8=ACCEPT" not in caps:
        return
    try:
        mail.enable("UTF8=ACCEPT")
    except imaplib.IMAP4.error:
        pass

def imap_mailbox_select_arg(mail: imaplib.IMAP4, folder: str) -> str:
    name = folder if mail.utf8_enabled else encode_imap_modified_utf7(folder)
    return quote_imap_mailbox(name)

def imap_connect(
    host: str,
    port: int,
    user: str,
    password: str,
    *,
    use_ssl: bool,
    use_starttls: bool,
) -> imaplib.IMAP4:
    if use_ssl and use_starttls:
        raise ValueError("Нельзя одновременно --ssl и --starttls")

    if use_ssl:
        mail = imaplib.IMAP4_SSL(host, port)
        _enable_tcp_keepalive(mail)
        mail.login(user, password)
    else:
        mail = imaplib.IMAP4(host, port)
        _enable_tcp_keepalive(mail)
        if use_starttls:
            ctx = ssl.create_default_context()
            mail.starttls(ssl_context=ctx)
            _enable_tcp_keepalive(mail)
        mail.login(user, password)
    try_enable_imap_utf8(mail)
    return mail

def select_folder(mail: imaplib.IMAP4, folder: str) -> None:
    status, data = mail.select(imap_mailbox_select_arg(mail, folder), readonly=True)
    if status != "OK":
        raise RuntimeError(
            f'Не удалось открыть папку "{folder}": {data}. '
            "Укажите верное имя (см. --list-folders)."
        )

def fetch_sent_uids(mail: imaplib.IMAP4, limit: Optional[int]) -> list[bytes]:
    status, data = mail.search(None, "ALL")
    if status != "OK" or not data or not data[0]:
        return []
    uids = data[0].split()
    if limit is not None and len(uids) > limit:
        uids = uids[-limit:]
    return uids

def iter_rfc822_messages(
    mail: imaplib.IMAP4, uids: list[bytes], *, use_imap_uid: bool
) -> Iterator[Tuple[bytes, Message]]:
    for uid in uids:
        if use_imap_uid:
            status, data = mail.uid("FETCH", uid, "(RFC822)")
        else:
            status, data = mail.fetch(uid, "(RFC822)")
        if status != "OK" or not data or not isinstance(data[0], tuple):
            continue
        raw = data[0][1]
        if not isinstance(raw, (bytes, bytearray)):
            continue
        msg = email.message_from_bytes(bytes(raw))
        yield uid, msg

def imap_supports_idle(mail: imaplib.IMAP4) -> bool:
    if not callable(getattr(mail, "idle", None)):
        return False
    try:
        mail.capability()
    except imaplib.IMAP4.error:
        pass
    caps = getattr(mail, "capabilities", ()) or ()
    for c in caps:
        if isinstance(c, bytes) and c.upper() == b"IDLE":
            return True
        if isinstance(c, str) and c.upper() == "IDLE":
            return True
    return False

def idle_wait_sent_folder(
    mail: imaplib.IMAP4, chunk_sec: float
) -> IdleWake:
    if not imap_supports_idle(mail):
        return "unsupported"
    saw_exists = False
    try:
        with mail.idle(duration=chunk_sec) as idler:
            for typ, _data in idler:
                t = typ.decode("ascii", errors="ignore") if isinstance(typ, bytes) else str(typ)
                if t.upper() == "EXISTS":
                    saw_exists = True
                    break
    except imaplib.IMAP4.error:
        return "unsupported"
    return "exists" if saw_exists else "timeout"

def list_folders(mail: imaplib.IMAP4) -> None:
    status, folders = mail.list()
    if status != "OK" or not folders:
        print("Не удалось получить список папок.", file=sys.stderr)
        return
    for raw in folders:
        line = raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else raw
        print(line)
