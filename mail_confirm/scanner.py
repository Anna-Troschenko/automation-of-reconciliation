from __future__ import annotations

import imaplib
import sqlite3
import sys
from typing import Optional, Tuple

from mail_confirm.db import (
    ensure_recipient_digest_row,
    get_last_sent_imap_uid,
    insert_confirmation_row,
    set_last_sent_imap_uid,
)
from mail_confirm.email_parse import (
    decode_mime_header,
    format_confirmation_line,
    get_text_body,
    is_outbound_digest_email,
    message_dedupe_key,
    parse_confirmation,
    primary_recipient_email,
)
from mail_confirm.imap_client import (
    fetch_sent_uids,
    fetch_uids_after,
    iter_rfc822_messages,
)


def scan_sent_and_store(
    mail: imaplib.IMAP4,
    conn: Optional[sqlite3.Connection],
    *,
    sent_folder: str,
    limit: Optional[int],
    use_uid_cursor: bool,
    stdout_only: bool,
    dry_run: bool,
    default_digest_interval: int,
) -> Tuple[int, int, int]:
    inserted = 0
    skipped = 0
    warned_no_recipient = False
    if conn is not None and use_uid_cursor:
        last_uid = get_last_sent_imap_uid(conn, sent_folder)
        uids = fetch_uids_after(mail, last_uid, limit)
    else:
        uids = fetch_sent_uids(mail, limit)

    for uid, msg in iter_rfc822_messages(mail, uids, use_imap_uid=use_uid_cursor):
        if is_outbound_digest_email(msg):
            continue
        body = get_text_body(msg)
        parsed = parse_confirmation(body)
        if not parsed:
            continue

        id_yav, id_sop = parsed
        subj = decode_mime_header(msg.get("Subject") or "")
        date_hdr = msg.get("Date")
        dedupe = message_dedupe_key(msg, sent_folder, uid)
        recipient = primary_recipient_email(msg)

        if stdout_only:
            print(format_confirmation_line(id_yav, id_sop))
            continue

        if dry_run:
            print(
                f"Найдено: явление={id_yav}, сопоставленный={id_sop} | To={recipient!r} | "
                f"{subj[:60]!r}"
            )
            continue

        if not recipient:
            if not warned_no_recipient:
                print(
                    "IMAP: обнаружено письмо с подтверждением, но без адреса получателя (To/Delivered-To пуст). "
                    "Такие письма не сохраняются в БД, т.к. для них невозможна отправка сводки.",
                    file=sys.stderr,
                )
                warned_no_recipient = True
            skipped += 1
            continue

        if conn is not None:
            ensure_recipient_digest_row(conn, recipient, default_digest_interval)
            if insert_confirmation_row(
                conn, dedupe, id_yav, id_sop, subj, date_hdr, recipient
            ):
                inserted += 1
            else:
                skipped += 1

    max_uid_in_mailbox = 0
    if conn is not None and use_uid_cursor:
        status, highest_data = mail.uid("SEARCH", None, "UID", "*")
        if status == "OK" and highest_data and highest_data[0]:
            try:
                max_uid_in_mailbox = int(highest_data[0].split()[-1])
            except (ValueError, IndexError):
                max_uid_in_mailbox = 0
        prev = get_last_sent_imap_uid(conn, sent_folder)
        if max_uid_in_mailbox > prev:
            set_last_sent_imap_uid(conn, sent_folder, max_uid_in_mailbox)

    return inserted, skipped, max_uid_in_mailbox
