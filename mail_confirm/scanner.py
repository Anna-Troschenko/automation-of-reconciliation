from __future__ import annotations

import imaplib
import sqlite3
import sys
from typing import Optional, Tuple

from mail_confirm.db import (
    confirmation_acceptable_for_recipient,
    get_last_sent_imap_uid,
    insert_confirmation_row,
    is_recipient_blocked,
    is_recipient_configured,
    recipient_can_accumulate,
    set_last_sent_imap_uid,
    sync_database_reads,
)
from mail_confirm.email_parse import (
    decode_mime_header,
    format_confirmation_line,
    get_text_body,
    has_end_of_reconciliation_marker,
    is_outbound_digest_email,
    message_dedupe_key,
    parse_append_reconciliation_id,
    parse_confirmations,
    primary_recipient_email,
)

from mail_confirm.triggers import check_outbound_keyword
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
    warned_unconfigured: set[str] = set()
    warned_pause_backlog: set[str] = set()
    if conn is not None:
        sync_database_reads(conn)
    if conn is not None and use_uid_cursor:
        last_uid = get_last_sent_imap_uid(conn, sent_folder)
        uids = fetch_uids_after(mail, last_uid, limit)
    else:
        uids = fetch_sent_uids(mail, limit)

    max_processed_uid = 0
    for uid, msg in iter_rfc822_messages(mail, uids, use_imap_uid=use_uid_cursor):
        if use_uid_cursor:
            try:
                u = int(uid.decode() if isinstance(uid, bytes) else uid)
                if u > max_processed_uid:
                    max_processed_uid = u
            except (ValueError, AttributeError):
                pass

        if is_outbound_digest_email(msg):
            continue
        body = get_text_body(msg)
        recipient_kw = primary_recipient_email(msg)
        if conn is not None and recipient_kw and not stdout_only and not dry_run:
            if recipient_can_accumulate(conn, recipient_kw) and check_outbound_keyword(
                conn, recipient_kw, body, default_digest_interval
            ):
                print(
                    f"IMAP: кодовое слово → немедленная сводка для {recipient_kw}",
                    file=sys.stderr,
                )
        confirmations = parse_confirmations(body)
        if not confirmations:
            continue

        subj = decode_mime_header(msg.get("Subject") or "")
        date_hdr = msg.get("Date")
        dedupe_base = message_dedupe_key(msg, sent_folder, uid)
        recipient = primary_recipient_email(msg)
        append_rid = parse_append_reconciliation_id(body)
        end_marker = has_end_of_reconciliation_marker(body)

        if stdout_only:
            for id_yav, id_sop, event_date in confirmations:
                print(format_confirmation_line(id_yav, id_sop, event_date))
            continue

        if dry_run:
            extra = f" | append→#{append_rid}" if append_rid else ""
            end_info = " | END" if end_marker else ""
            for id_yav, id_sop, event_date in confirmations:
                date_info = f" | дата явления={event_date}" if event_date else ""
                print(
                    f"Найдено: явление={id_yav}, сопоставленный={id_sop}{date_info} | "
                    f"To={recipient!r} | {subj[:60]!r}{extra}{end_info}"
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
            skipped += len(confirmations)
            continue

        if conn is not None:
            if is_recipient_blocked(conn, recipient):
                print(
                    f"IMAP: пропуск удалённого получателя {recipient}",
                    file=sys.stderr,
                )
                skipped += len(confirmations)
                continue
            if not is_recipient_configured(conn, recipient):
                if recipient not in warned_unconfigured:
                    print(
                        f"IMAP: пропуск — получатель {recipient} не настроен в веб-интерфейсе",
                        file=sys.stderr,
                    )
                    warned_unconfigured.add(recipient)
                skipped += len(confirmations)
                continue
            if not recipient_can_accumulate(conn, recipient):
                skipped += len(confirmations)
                continue
            if not confirmation_acceptable_for_recipient(conn, recipient, date_hdr):
                if recipient not in warned_pause_backlog:
                    print(
                        f"IMAP: пропуск письма до возобновления накопления для {recipient}",
                        file=sys.stderr,
                    )
                    warned_pause_backlog.add(recipient)
                skipped += len(confirmations)
                continue
            if append_rid is not None:
                from mail_confirm.reconciliations import reconciliation_belongs_to

                if not reconciliation_belongs_to(conn, append_rid, recipient):
                    print(
                        f"IMAP: «Дополнение в сверку {append_rid}» — сверка не найдена "
                        f"у {recipient}, привязка к открытой сверке.",
                        file=sys.stderr,
                    )
                    append_rid = None

            inserted_in_msg = 0
            for idx, (id_yav, id_sop, event_date) in enumerate(confirmations):
                row_key = dedupe_base if idx == 0 else f"{dedupe_base}#part{idx}"
                if insert_confirmation_row(
                    conn,
                    row_key,
                    id_yav,
                    id_sop,
                    subj,
                    date_hdr,
                    recipient,
                    append_reconciliation_id=append_rid,
                    event_date=event_date,
                ):
                    inserted += 1
                    inserted_in_msg += 1
                else:
                    skipped += 1
            if inserted_in_msg and append_rid is not None:
                print(
                    f"IMAP: добавлено {inserted_in_msg} подтвержд. в сверку #{append_rid} "
                    f"для {recipient}",
                    file=sys.stderr,
                )
            if end_marker and inserted_in_msg:
                from mail_confirm.db import request_immediate_digest

                request_immediate_digest(conn, recipient)
                print(
                    f"IMAP: «Окончание редактирования сверки» от {recipient} — "
                    f"запрошена немедленная отправка сводки.",
                    file=sys.stderr,
                )

    max_uid_in_mailbox = 0
    if conn is not None and use_uid_cursor:
        status, highest_data = mail.uid("SEARCH", None, "UID", "*")
        if status == "OK" and highest_data and highest_data[0]:
            try:
                max_uid_in_mailbox = int(highest_data[0].split()[-1])
            except (ValueError, IndexError):
                max_uid_in_mailbox = 0
        prev = get_last_sent_imap_uid(conn, sent_folder)
        new_cursor = max(prev, max_uid_in_mailbox, max_processed_uid)
        if new_cursor > prev:
            set_last_sent_imap_uid(conn, sent_folder, new_cursor)


    return inserted, skipped, max_uid_in_mailbox
