from __future__ import annotations

import smtplib
import ssl
import sys
from datetime import datetime, timezone
from email.message import EmailMessage
from typing import Sequence

import sqlite3

from mail_confirm.constants import DIGEST_SMTP_SUBJECT
from mail_confirm.db import (
    collect_pending_recipients,
    digest_due,
    get_recipient_interval,
)
from mail_confirm.email_parse import format_confirmation_line
from mail_confirm.utils import utc_now_sql


def default_smtp_host(imap_host: str) -> str:
    h = imap_host.lower()
    if "gmail.com" in h:
        return "smtp.gmail.com"
    return imap_host


def send_digest_email(
    *,
    smtp_host: str,
    smtp_port: int,
    smtp_user: str,
    smtp_password: str,
    mail_from: str,
    recipient: str,
    lines: Sequence[str],
) -> None:
    msg = EmailMessage()
    msg["Subject"] = DIGEST_SMTP_SUBJECT
    msg["From"] = mail_from
    msg["To"] = recipient
    msg.set_content("\n".join(lines) + "\n", charset="utf-8")
    with smtplib.SMTP(smtp_host, smtp_port, timeout=60) as smtp:
        smtp.starttls(context=ssl.create_default_context())
        smtp.login(smtp_user, smtp_password)
        smtp.send_message(msg)


def warn_digest_interval_waiting(
    conn: sqlite3.Connection, default_interval: int, *, after_new_inserts: int, sent: int
) -> None:
    if after_new_inserts <= 0 or sent > 0:
        return
    recs = collect_pending_recipients(conn)
    if not recs:
        return
    now = datetime.now(timezone.utc)
    if any(digest_due(conn, r, default_interval, now) for r in recs):
        return
    print(
        "SMTP: для получателей с известным e-mail в БД есть неотправленные сводки, "
        "но интервал ещё не истёк (max(последняя сводка, первое неотправленное в БД) + N сек). "
        "Строки без recipient_email сюда не входят. След. проверка — EXISTS или конец IDLE.",
        file=sys.stderr,
    )


def send_due_digests(
    conn: sqlite3.Connection,
    *,
    default_interval: int,
    smtp_host: str,
    smtp_port: int,
    smtp_user: str,
    smtp_password: str,
    mail_from: str,
    dry_run: bool,
) -> int:
    now = datetime.now(timezone.utc)
    sent = 0
    for recipient in collect_pending_recipients(conn):
        if not digest_due(conn, recipient, default_interval, now):
            continue
        rows = conn.execute(
            """
            SELECT id, id_yavleniya, id_sopostavlennyi FROM confirmations
            WHERE recipient_email = ? COLLATE NOCASE AND digest_sent_at IS NULL
            ORDER BY id
            """,
            (recipient,),
        ).fetchall()
        if not rows:
            continue
        lines = [
            format_confirmation_line(int(r["id_yavleniya"]), int(r["id_sopostavlennyi"]))
            for r in rows
        ]
        ids = [int(r["id"]) for r in rows]
        if dry_run:
            print(f"[dry-run] сводка для {recipient}: {len(lines)} строк(и)", file=sys.stderr)
            continue
        try:
            send_digest_email(
                smtp_host=smtp_host,
                smtp_port=smtp_port,
                smtp_user=smtp_user,
                smtp_password=smtp_password,
                mail_from=mail_from,
                recipient=recipient,
                lines=lines,
            )
            print(
                f"SMTP: сводка отправлена → To: {recipient} ({len(lines)} подтвержд.), From: {mail_from}",
                file=sys.stderr,
            )
            when = utc_now_sql()
            conn.executemany(
                "UPDATE confirmations SET digest_sent_at = ? WHERE id = ?",
                [(when, i) for i in ids],
            )
            conn.execute(
                """
                INSERT INTO recipient_digest (email, interval_seconds, last_digest_sent_at)
                VALUES (?, ?, ?)
                ON CONFLICT(email) DO UPDATE SET last_digest_sent_at = excluded.last_digest_sent_at
                """,
                (recipient.lower(), get_recipient_interval(conn, recipient, default_interval), when),
            )
            conn.commit()
            sent += 1
        except Exception as e:
            print(f"SMTP ошибка при отправке сводки для {recipient}: {e}", file=sys.stderr)
    return sent
