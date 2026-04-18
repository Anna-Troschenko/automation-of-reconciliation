from __future__ import annotations

import re
from email.header import decode_header
from email.message import Message
from email.utils import getaddresses, parseaddr
from typing import Optional, Tuple

from mail_confirm.constants import CONFIRMATION_PATTERN, DIGEST_SMTP_SUBJECT


def decode_mime_header(value: str) -> str:
    parts: list[str] = []
    for chunk, enc in decode_header(value):
        if isinstance(chunk, bytes):
            parts.append(chunk.decode(enc or "utf-8", errors="replace"))
        else:
            parts.append(chunk)
    return "".join(parts)


def is_outbound_digest_email(msg: Message) -> bool:
    subj = decode_mime_header(msg.get("Subject") or "").strip()
    return subj == DIGEST_SMTP_SUBJECT


def get_text_body(msg: Message) -> str:
    texts: list[str] = []

    def walk(part: Message) -> None:
        ctype = part.get_content_type()
        if ctype == "text/plain":
            payload = part.get_payload(decode=True)
            if payload:
                charset = part.get_content_charset() or "utf-8"
                texts.append(payload.decode(charset, errors="replace"))
        elif ctype == "text/html":
            payload = part.get_payload(decode=True)
            if payload:
                charset = part.get_content_charset() or "utf-8"
                raw = payload.decode(charset, errors="replace")
                no_tags = re.sub(r"<[^>]+>", " ", raw)
                texts.append(no_tags)

    if msg.is_multipart():
        for p in msg.walk():
            if p.get_content_maintype() == "multipart":
                continue
            walk(p)
    else:
        walk(msg)

    return "\n".join(texts)


def parse_confirmation(text: str) -> Optional[Tuple[int, int]]:
    m = CONFIRMATION_PATTERN.search(text.replace("\r\n", "\n"))
    if not m:
        return None
    return int(m.group(1)), int(m.group(2))


def format_confirmation_line(id_yav: int, id_sop: int) -> str:
    return (
        f"Добрый день! Подтверждаю нежелательное явление {id_yav}, "
        f"сопоставленный ID: {id_sop}"
    )


def _header_joined(msg: Message, name: str) -> str:
    parts = msg.get_all(name, [])
    if parts:
        return " ".join(decode_mime_header(str(p)) for p in parts if p)
    v = msg.get(name)
    return decode_mime_header(v) if v else ""


def _first_email_from_raw_header(raw: str) -> str:
    if not raw:
        return ""
    raw = decode_mime_header(raw)
    raw = raw.replace("\r\n", " ").replace("\n", " ")
    for _name, addr in getaddresses([raw]):
        a = (addr or "").strip()
        if "@" in a:
            return a.lower()
    _, single = parseaddr(raw)
    if "@" in single:
        return single.strip().lower()
    return ""


def primary_recipient_email(msg: Message) -> str:
    for key in (
        "To",
        "Delivered-To",
        "Envelope-To",
        "X-Original-To",
        "X-Forwarded-To",
    ):
        combined = _header_joined(msg, key)
        if combined:
            found = _first_email_from_raw_header(combined)
            if found:
                return found
    cc = _header_joined(msg, "Cc")
    if cc:
        found = _first_email_from_raw_header(cc)
        if found:
            return found
    return ""


def message_dedupe_key(msg: Message, folder: str, uid: bytes) -> str:
    mid = (msg.get("Message-ID") or "").strip()
    if mid:
        return mid
    return f"imap:{folder}:{uid.decode()}"
