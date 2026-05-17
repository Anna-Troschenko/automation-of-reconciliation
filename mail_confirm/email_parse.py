from __future__ import annotations

import re
from email.header import decode_header
from email.message import Message
from email.utils import getaddresses, parseaddr
from typing import Optional, Tuple

from mail_confirm.constants import (
    APPEND_RECONCILIATION_PATTERN,
    CONFIRMATION_PATTERN,
    DELETION_PATTERN,
    DIGEST_SMTP_SUBJECT,
    END_OF_RECONCILIATION_PATTERN,
)

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
    if subj == DIGEST_SMTP_SUBJECT:
        return True

    return subj.startswith(DIGEST_SMTP_SUBJECT + " #")

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

ParsedEntry = Tuple[str, str, Optional[str], Optional[str]]

def _parse_pattern_matches(
    pattern: re.Pattern[str], text: str
) -> list[ParsedEntry]:
    if not text:
        return []
    out: list[ParsedEntry] = []
    seen: set[ParsedEntry] = set()
    for m in pattern.finditer(text.replace("\r\n", "\n")):
        id_yav = m.group(1).strip()
        id_sop = m.group(2).strip()
        raw_event = (m.group(3) or "").strip()
        raw_received = (m.group(4) or "").strip()
        event_date = _normalize_dmy_to_iso(raw_event) if raw_event else None
        received_date = _normalize_dmy_to_iso(raw_received) if raw_received else None
        key = (id_yav, id_sop, event_date, received_date)
        if key in seen:
            continue
        seen.add(key)
        out.append(key)
    return out

def parse_confirmations(text: str) -> list[ParsedEntry]:
    """Найти все строки подтверждений в письме. Возвращает список кортежей
    (id_yavleniya, id_sopostavlennyi, event_date_iso_or_None, received_date_iso_or_None)."""
    return _parse_pattern_matches(CONFIRMATION_PATTERN, text)

def parse_confirmation(text: str) -> Optional[ParsedEntry]:
    """Первое подтверждение из письма (back-compat). См. parse_confirmations."""
    items = parse_confirmations(text)
    return items[0] if items else None

def parse_deletions(text: str) -> list[ParsedEntry]:
    """Найти все строки «Удаление нежелательного явления …» в письме.

    Возвращает список кортежей `(id_yavleniya, id_sopostavlennyi,
    event_date_iso_or_None, received_date_iso_or_None)` — структура такая же,
    как у `parse_confirmations`, чтобы было удобно матчить удаляемые строки
    против уже сохранённых в БД подтверждений."""
    return _parse_pattern_matches(DELETION_PATTERN, text)

def has_end_of_reconciliation_marker(text: str) -> bool:
    """В конце письма стоит «Окончание редактирования сверки.» —
    значит сверку нужно отправить немедленно после применения дополнений."""
    if not text:
        return False
    return END_OF_RECONCILIATION_PATTERN.search(text.replace("\r\n", "\n")) is not None

def _normalize_dmy_to_iso(value: str) -> Optional[str]:
    """«16.05.2026» / «16/5/26» / «16-05-2026» → «2026-05-16»."""
    if not value:
        return None
    parts = re.split(r"[.\-/]", value.strip())
    if len(parts) != 3:
        return None
    try:
        d, mo, y = (int(p) for p in parts)
    except ValueError:
        return None
    if y < 100:
        y += 2000
    if not (1 <= d <= 31 and 1 <= mo <= 12 and 1900 <= y <= 2999):
        return None
    return f"{y:04d}-{mo:02d}-{d:02d}"

def parse_append_reconciliation_id(text: str) -> Optional[int]:
    """Найти префикс «Дополнение в сверку <id>» в теле письма."""
    if not text:
        return None
    m = APPEND_RECONCILIATION_PATTERN.search(text.replace("\r\n", "\n"))
    if not m:
        return None
    try:
        return int(m.group(1))
    except (TypeError, ValueError):
        return None

def format_confirmation_line(
    id_yav: str | int,
    id_sop: str | int,
    event_date: Optional[str] = None,
    received_date: Optional[str] = None,
) -> str:
    """Сформировать строку подтверждения. Даты — в ISO YYYY-MM-DD;
    в строку будет вписан формат ДД.ММ.ГГГГ."""
    base = (
        f"Добрый день! Подтверждаю нежелательное явление {id_yav}, "
        f"сопоставленный ID: {id_sop}"
    )
    parts: list[str] = []
    if event_date:
        parts.append(f"Дата явления: {_iso_to_dmy(event_date)}")
    if received_date:
        parts.append(f"Дата получения: {_iso_to_dmy(received_date)}")
    if parts:
        return f"{base}. " + ". ".join(parts) + "."
    return base

def format_deletion_line(
    id_yav: str | int,
    id_sop: str | int,
    event_date: Optional[str] = None,
    received_date: Optional[str] = None,
) -> str:
    base = (
        f"Добрый день! Удаление нежелательного явления {id_yav}, "
        f"сопоставленный ID: {id_sop}"
    )
    parts: list[str] = []
    if event_date:
        parts.append(f"Дата явления: {_iso_to_dmy(event_date)}")
    if received_date:
        parts.append(f"Дата получения: {_iso_to_dmy(received_date)}")
    if parts:
        return f"{base}. " + ". ".join(parts) + "."
    return base

def sent_at_to_iso_date(value: Optional[str]) -> Optional[str]:
    """Привести `sent_at` из БД к ISO `YYYY-MM-DD` для форматирования строк."""
    if not value:
        return None
    s = str(value).strip()
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return s[:10]
    from mail_confirm.utils import parse_stored_sent_at

    dt = parse_stored_sent_at(s)
    return dt.strftime("%Y-%m-%d") if dt else None

def _iso_to_dmy(iso: str) -> str:
    try:
        y, mo, d = iso.split("-")
        return f"{int(d):02d}.{int(mo):02d}.{int(y):04d}"
    except (ValueError, AttributeError):
        return iso

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
