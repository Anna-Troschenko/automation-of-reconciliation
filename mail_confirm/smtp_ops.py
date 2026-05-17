from __future__ import annotations

import smtplib
import ssl
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from email.message import EmailMessage
from typing import Any, Optional, Sequence

import sqlite3

from mail_confirm.constants import DIGEST_SMTP_SUBJECT
from mail_confirm.email_parse import format_confirmation_line

from mail_confirm.db import (
    clear_immediate_digest,
    collect_pending_recipients,
    get_digest_intro_text,
    is_recipient_blocked,
    sync_database_reads,
    touch_last_digest_sent,
)

from mail_confirm.reconciliations import (
    confirmation_lines_for_reconciliation,
    confirmation_rows_for_reconciliation,
    get_open_reconciliation_id,
    get_reconciliation,
    link_orphan_pending_to_open,
    list_reconciliations_for_recipient,
    mark_reconciliation_sent,
)
from mail_confirm.triggers import digest_due_for_recipient
from mail_confirm.utils import parse_email_date_header, parse_sql_datetime, utc_now_sql

@dataclass(frozen=True)
class SmtpConfig:
    host: str
    port: int
    user: str
    password: str
    mail_from: str

def default_smtp_host(imap_host: str) -> str:
    h = imap_host.lower()
    if "gmail.com" in h:
        return "smtp.gmail.com"
    return imap_host

def _fmt_dmy(value: Optional[str]) -> str:
    """Любое значение `event_date` / `sent_at` / `inserted_at` → `ДД.ММ.ГГГГ` или ''."""
    if not value:
        return ""
    s = str(value).strip()

    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        try:
            y, mo, d = int(s[0:4]), int(s[5:7]), int(s[8:10])
            return f"{d:02d}.{mo:02d}.{y:04d}"
        except ValueError:
            pass

    try:
        dt = parse_email_date_header(s)
        if dt is not None:
            return dt.strftime("%d.%m.%Y")
    except Exception:
        pass

    try:
        dt = parse_sql_datetime(s)
        return dt.strftime("%d.%m.%Y")
    except (ValueError, AttributeError):
        return s

def _esc(s: str) -> str:
    return (
        s.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )

def _build_html_table(rows: Sequence[Any]) -> str:
    """rows — последовательность sqlite3.Row с полями
    id_yavleniya, id_sopostavlennyi, event_date, sent_at, inserted_at."""

    head = (
        "<tr>"
        "<th style=\"padding:5px 8px;border:1px solid #ccc;text-align:left;background:#f4f6fa;\">ID нежелательного явления</th>"
        "<th style=\"padding:5px 8px;border:1px solid #ccc;text-align:left;background:#f4f6fa;\">Сопоставленный ID</th>"
        "<th style=\"padding:5px 8px;border:1px solid #ccc;text-align:left;background:#f4f6fa;\">Дата получения нежелательного явления</th>"
        "<th style=\"padding:5px 8px;border:1px solid #ccc;text-align:left;background:#f4f6fa;\">Дата нежелательного явления</th>"
        "<th style=\"padding:5px 8px;border:1px solid #ccc;text-align:left;background:#f4f6fa;\">Корректность</th>"
        "</tr>"
    )
    body_rows: list[str] = []
    for r in rows:

        received = _fmt_dmy(r["sent_at"]) or _fmt_dmy(r["inserted_at"])
        event = _fmt_dmy(r["event_date"])
        id_yav = _esc(str(r["id_yavleniya"] or ""))
        id_sop = _esc(str(r["id_sopostavlennyi"] or ""))
        body_rows.append(
            "<tr>"
            f"<td style=\"padding:5px 8px;border:1px solid #ccc;font-family:ui-monospace,monospace;\">{id_yav}</td>"
            f"<td style=\"padding:5px 8px;border:1px solid #ccc;font-family:ui-monospace,monospace;\">{id_sop}</td>"
            f"<td style=\"padding:5px 8px;border:1px solid #ccc;\">{_esc(received)}</td>"
            f"<td style=\"padding:5px 8px;border:1px solid #ccc;\">{_esc(event)}</td>"

            "<td style=\"padding:5px 8px;border:1px solid #ccc;min-width:110px;\">&nbsp;</td>"
            "</tr>"
        )

    return (
        "<table style=\"border-collapse:collapse;border:1px solid #ccc;"
        "font-family:-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;font-size:12px;\">"
        + head
        + "".join(body_rows)
        + "</table>"
    )

def _build_plain_table(rows: Sequence[Any]) -> str:
    """Plain-fallback: моноширинная таблица для клиентов без HTML."""
    headers = (
        "ID явления",
        "Сопоставленный ID",
        "Дата получения",
        "Дата явления",
        "Корректность",
    )
    body: list[tuple[str, ...]] = []
    for r in rows:
        body.append(
            (
                str(r["id_yavleniya"] or ""),
                str(r["id_sopostavlennyi"] or ""),
                _fmt_dmy(r["sent_at"]) or _fmt_dmy(r["inserted_at"]),
                _fmt_dmy(r["event_date"]),
                "",
            )
        )

    cols = list(zip(headers, *body))
    widths = [max(len(c) for c in col) for col in cols]
    sep = "+-" + "-+-".join("-" * w for w in widths) + "-+"
    def fmt(row: Sequence[str]) -> str:
        return "| " + " | ".join(v.ljust(widths[i]) for i, v in enumerate(row)) + " |"
    lines = [sep, fmt(headers), sep, *(fmt(r) for r in body), sep]
    return "\n".join(lines)

def _intro_to_html(text: str) -> str:
    """Конвертация многострочного текста в HTML с сохранением переносов."""
    paragraphs = [p.strip() for p in text.split("\n\n")]
    return "".join(
        f"<p>{_esc(p).replace(chr(10), '<br>')}</p>" for p in paragraphs if p
    )


def send_digest_email(
    *,
    smtp_host: str,
    smtp_port: int,
    smtp_user: str,
    smtp_password: str,
    mail_from: str,
    recipient: str,
    lines: Sequence[str],
    reconciliation_id: Optional[int] = None,
    rows: Optional[Sequence[Any]] = None,
    intro_text: Optional[str] = None,
) -> None:

    """Если переданы `rows` (sqlite3.Row из confirmations), письмо отправляется
    в виде HTML-таблицы с plain-fallback. Иначе — старое поведение (текст)."""
    msg = EmailMessage()
    if reconciliation_id is not None:
        msg["Subject"] = f"{DIGEST_SMTP_SUBJECT} #{reconciliation_id}"
    else:
        msg["Subject"] = DIGEST_SMTP_SUBJECT
    msg["From"] = mail_from
    msg["To"] = recipient
    if reconciliation_id is not None:
        msg["X-Reconciliation-Id"] = str(reconciliation_id)

    intro = (intro_text or "").strip()

    if rows is not None and len(rows) > 0:
        intro_plain: list[str] = []
        intro_html: list[str] = []
        if intro:
            intro_plain.extend([intro, ""])
            intro_html.append(_intro_to_html(intro))
        if reconciliation_id is not None:
            intro_plain.extend(
                [
                    f"ID сверки: {reconciliation_id}",
                    "",
                ]
            )
            intro_html.append(
                f"<p><strong>ID сверки: {reconciliation_id}</strong></p>"
            )

        plain_body = "\n".join(intro_plain) + _build_plain_table(rows) + "\n"
        html_body = (
            "<!doctype html><html><body>"
            + "".join(intro_html)
            + _build_html_table(rows)
            + "</body></html>"
        )
        msg.set_content(plain_body, charset="utf-8")
        msg.add_alternative(html_body, subtype="html")
    else:
        body_parts: list[str] = []
        if intro:
            body_parts.extend([intro, ""])
        if reconciliation_id is not None:
            body_parts.extend([f"ID сверки: {reconciliation_id}", ""])
        body_parts.extend(lines)

        msg.set_content("\n".join(body_parts) + "\n", charset="utf-8")


    with smtplib.SMTP(smtp_host, smtp_port, timeout=60) as smtp:
        smtp.starttls(context=ssl.create_default_context())
        smtp.login(smtp_user, smtp_password)
        smtp.send_message(msg)

def send_reconciliation_by_id(
    conn: sqlite3.Connection,
    reconciliation_id: int,
    smtp: SmtpConfig,
    *,
    allow_resend: bool = True,
) -> int:
    row = get_reconciliation(conn, reconciliation_id)
    if row is None:
        raise ValueError("Сверка не найдена")
    recipient = str(row["recipient_email"])
    if is_recipient_blocked(conn, recipient):
        raise ValueError("Получатель удалён")
    if row["sent_at"] is not None and not allow_resend:
        raise ValueError("Сверка уже отправлена")

    if row["sent_at"] is not None:
        pending = conn.execute(
            """
            SELECT COUNT(*) AS c FROM confirmations
            WHERE reconciliation_id = ? AND digest_sent_at IS NULL
            """,
            (reconciliation_id,),
        ).fetchone()
        if not pending or int(pending["c"]) == 0:
            raise ValueError("Нет новых писем для отправки в этой сверке")
    lines = confirmation_lines_for_reconciliation(conn, reconciliation_id)
    if not lines:
        raise ValueError("В сверке нет писем")
    rows = confirmation_rows_for_reconciliation(conn, reconciliation_id)

    send_digest_email(
        smtp_host=smtp.host,
        smtp_port=smtp.port,
        smtp_user=smtp.user,
        smtp_password=smtp.password,
        mail_from=smtp.mail_from,
        recipient=recipient,
        lines=lines,
        reconciliation_id=reconciliation_id,
        rows=rows,
        intro_text=get_digest_intro_text(conn),
    )
    when = utc_now_sql()

    mark_reconciliation_sent(conn, reconciliation_id, when)

    touch_last_digest_sent(conn, recipient, when)
    clear_immediate_digest(conn, recipient)
    conn.commit()
    return len(lines)

def warn_digest_interval_waiting(
    conn: sqlite3.Connection, default_interval: int, *, after_new_inserts: int, sent: int
) -> None:
    if after_new_inserts <= 0 or sent > 0:
        return
    recs = collect_pending_recipients(conn)
    if not recs:
        return
    now = datetime.now(timezone.utc)
    if any(digest_due_for_recipient(conn, r, default_interval, now) for r in recs):
        return
    print(
        "SMTP: для получателей с известным e-mail в БД есть неотправленные сводки, "
        "но интервал ещё не истёк.",
        file=sys.stderr,
    )

def _pending_reconciliations_for_recipient(
    conn: sqlite3.Connection,
    recipient: str,
    *,
    include_closed: bool,
) -> list[tuple[int, bool]]:
    """Вернёт [(rid, is_open), …] для всех сверок получателя, в которых
    есть подтверждения с digest_sent_at IS NULL.

    Если include_closed=False — закрытые сверки (sent_at IS NOT NULL)
    пропускаются: дополнения в них отправляются только по явному
    immediate-сигналу (маркер «Окончание редактирования сверки.»,
    keyword, кнопка в UI), а не по обычным триггерам времени/порога."""
    from mail_confirm.db import normalize_recipient_email

    em = normalize_recipient_email(recipient)
    if include_closed:
        where_extra = ""
    else:
        where_extra = "AND r.sent_at IS NULL"
    rows = conn.execute(
        f"""
        SELECT r.id AS rid, r.sent_at AS sent_at
        FROM reconciliations r
        WHERE r.recipient_email = ? COLLATE NOCASE
          {where_extra}
          AND EXISTS (
              SELECT 1 FROM confirmations c
              WHERE c.reconciliation_id = r.id AND c.digest_sent_at IS NULL
          )
        ORDER BY r.id
        """,
        (em,),
    ).fetchall()
    return [(int(r["rid"]), r["sent_at"] is None) for r in rows]



def _pending_rows_for_reconciliation(
    conn: sqlite3.Connection, reconciliation_id: int
) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT id_yavleniya, id_sopostavlennyi, event_date, sent_at, inserted_at
        FROM confirmations
        WHERE reconciliation_id = ? AND digest_sent_at IS NULL
        ORDER BY id
        """,
        (reconciliation_id,),
    ).fetchall()


def _mark_pending_sent_for_reconciliation(
    conn: sqlite3.Connection, reconciliation_id: int, when: str
) -> None:
    conn.execute(
        """
        UPDATE confirmations SET digest_sent_at = ?
        WHERE reconciliation_id = ? AND digest_sent_at IS NULL
        """,
        (when, reconciliation_id),
    )


def _send_pending_for_recipient(
    conn: sqlite3.Connection,
    recipient: str,
    *,
    smtp_host: str,
    smtp_port: int,
    smtp_user: str,
    smtp_password: str,
    mail_from: str,
    dry_run: bool,
    immediate: bool,
) -> bool:
    """Шлёт сводку по каждой сверке получателя, где есть pending-строки.
    Открытую сверку закрывает (sent_at). У закрытой — отправляет «дополнение»
    только из новых строк, не трогая её sent_at.

    immediate=True — отправляем все pending, в т.ч. в закрытые сверки.
    immediate=False — обычные триггеры (interval/schedule/count/keyword):
    трогаем только открытую сверку, дополнения в закрытые НЕ шлём."""
    link_orphan_pending_to_open(conn, recipient)
    targets = _pending_reconciliations_for_recipient(
        conn, recipient, include_closed=immediate
    )
    if not targets:
        return False


    any_sent = False
    when = utc_now_sql()
    for rid, is_open in targets:
        rows = confirmation_rows_for_reconciliation(conn, rid)
        lines = confirmation_lines_for_reconciliation(conn, rid)
        if not lines:

            continue
        if dry_run:
            kind = "сводка" if is_open else "дополнение"
            print(
                f"[dry-run] {kind} #{rid} для {recipient}: {len(lines)} строк(и)",
                file=sys.stderr,
            )
            any_sent = True
            continue
        send_digest_email(
            smtp_host=smtp_host,
            smtp_port=smtp_port,
            smtp_user=smtp_user,
            smtp_password=smtp_password,
            mail_from=mail_from,
            recipient=recipient,
            lines=lines,
            reconciliation_id=rid,
            rows=rows,
            intro_text=get_digest_intro_text(conn),
        )
        kind = "сводка" if is_open else "дополнение"

        print(
            f"SMTP: {kind} #{rid} → {recipient} ({len(lines)} подтвержд.), From: {mail_from}",
            file=sys.stderr,
        )
        if is_open:
            mark_reconciliation_sent(conn, rid, when)
        else:
            _mark_pending_sent_for_reconciliation(conn, rid, when)
        any_sent = True

    if any_sent and not dry_run:
        touch_last_digest_sent(conn, recipient, when)
        clear_immediate_digest(conn, recipient)
        conn.commit()
    return any_sent


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
    from mail_confirm.triggers import load_recipient_trigger

    sync_database_reads(conn)
    now = datetime.now(timezone.utc)
    sent = 0
    for recipient in collect_pending_recipients(conn):
        if is_recipient_blocked(conn, recipient):
            continue
        if not digest_due_for_recipient(conn, recipient, default_interval, now):
            continue
        rt = load_recipient_trigger(conn, recipient, default_interval)
        immediate = bool(rt and rt.immediate_digest)
        if _send_pending_for_recipient(
            conn,
            recipient,
            smtp_host=smtp_host,
            smtp_port=smtp_port,
            smtp_user=smtp_user,
            smtp_password=smtp_password,
            mail_from=mail_from,
            dry_run=dry_run,
            immediate=immediate,
        ):
            sent += 1
    return sent


def smtp_config_from_env(
    *,
    smtp_host: Optional[str],
    smtp_port: int,
    smtp_user: Optional[str],
    smtp_password: Optional[str],
    mail_from: Optional[str],
    imap_host: Optional[str] = None,
) -> Optional[SmtpConfig]:
    host = smtp_host or (default_smtp_host(imap_host) if imap_host else None)
    user = smtp_user or ""
    password = smtp_password or ""
    frm = mail_from or user
    if not host or not user or not password:
        return None
    return SmtpConfig(host=host, port=smtp_port, user=user, password=password, mail_from=frm or user)
