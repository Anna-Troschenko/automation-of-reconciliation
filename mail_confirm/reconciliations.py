from __future__ import annotations

import sqlite3
from typing import Any, Optional

from mail_confirm.email_parse import format_confirmation_line
from mail_confirm.utils import parse_email_date_header, utc_now_sql

def get_open_reconciliation_id(conn: sqlite3.Connection, recipient_email: str) -> Optional[int]:
    from mail_confirm.db import normalize_recipient_email

    em = normalize_recipient_email(recipient_email)
    row = conn.execute(
        """
        SELECT id FROM reconciliations
        WHERE recipient_email = ? COLLATE NOCASE AND sent_at IS NULL
        ORDER BY id DESC LIMIT 1
        """,
        (em,),
    ).fetchone()
    return int(row["id"]) if row else None

def reconciliation_belongs_to(
    conn: sqlite3.Connection, reconciliation_id: int, recipient_email: str
) -> bool:
    """Существует ли сверка с таким id у указанного получателя."""
    from mail_confirm.db import normalize_recipient_email

    em = normalize_recipient_email(recipient_email)
    row = conn.execute(
        """
        SELECT 1 FROM reconciliations
        WHERE id = ? AND recipient_email = ? COLLATE NOCASE
        """,
        (reconciliation_id, em),
    ).fetchone()
    return row is not None

def get_or_create_open_reconciliation(
    conn: sqlite3.Connection, recipient_email: str, *, letter_sent_hdr: Optional[str]
) -> int:
    from mail_confirm.db import normalize_recipient_email

    em = normalize_recipient_email(recipient_email)
    rid = get_open_reconciliation_id(conn, em)
    letter_ts = utc_now_sql()
    if letter_sent_hdr:
        dt = parse_email_date_header(letter_sent_hdr)
        if dt is not None:
            letter_ts = dt.astimezone(__import__("datetime").timezone.utc).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
    if rid is not None:
        conn.execute(
            """
            UPDATE reconciliations SET started_at = MIN(started_at, ?)
            WHERE id = ?
            """,
            (letter_ts, rid),
        )
        return rid
    cur = conn.execute(
        """
        INSERT INTO reconciliations (recipient_email, started_at, sent_at)
        VALUES (?, ?, NULL)
        """,
        (em, letter_ts),
    )
    return int(cur.lastrowid)

def refresh_reconciliation_started_at(conn: sqlite3.Connection, reconciliation_id: int) -> None:
    row = conn.execute(
        """
        SELECT MIN(
            COALESCE(
                NULLIF(TRIM(sent_at), ''),
                inserted_at
            )
        ) AS started
        FROM confirmations WHERE reconciliation_id = ?
        """,
        (reconciliation_id,),
    ).fetchone()
    if row and row["started"]:
        conn.execute(
            "UPDATE reconciliations SET started_at = ? WHERE id = ?",
            (str(row["started"]), reconciliation_id),
        )

def attach_confirmation_to_reconciliation(
    conn: sqlite3.Connection, confirmation_id: int, reconciliation_id: int
) -> None:
    conn.execute(
        "UPDATE confirmations SET reconciliation_id = ? WHERE id = ?",
        (reconciliation_id, confirmation_id),
    )
    refresh_reconciliation_started_at(conn, reconciliation_id)

def link_orphan_pending_to_open(conn: sqlite3.Connection, recipient_email: str) -> Optional[int]:
    from mail_confirm.db import normalize_recipient_email

    em = normalize_recipient_email(recipient_email)
    rid = get_or_create_open_reconciliation(conn, em, letter_sent_hdr=None)
    conn.execute(
        """
        UPDATE confirmations SET reconciliation_id = ?
        WHERE recipient_email = ? COLLATE NOCASE
          AND digest_sent_at IS NULL AND reconciliation_id IS NULL
        """,
        (rid, em),
    )
    refresh_reconciliation_started_at(conn, rid)
    return rid

def get_reconciliation(conn: sqlite3.Connection, reconciliation_id: int) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM reconciliations WHERE id = ?", (reconciliation_id,)
    ).fetchone()

def list_reconciliations_for_recipient(
    conn: sqlite3.Connection, recipient_email: str
) -> list[dict[str, Any]]:
    from mail_confirm.db import normalize_recipient_email

    em = normalize_recipient_email(recipient_email)
    rows = conn.execute(
        """
        SELECT r.id, r.recipient_email, r.started_at, r.sent_at,
               (SELECT COUNT(*) FROM confirmations c
                WHERE c.reconciliation_id = r.id) AS letter_count,
               (SELECT COUNT(*) FROM confirmations c
                WHERE c.reconciliation_id = r.id
                  AND c.digest_sent_at IS NULL) AS pending_count,
               (SELECT COUNT(DISTINCT c.digest_sent_at) FROM confirmations c
                WHERE c.reconciliation_id = r.id
                  AND c.digest_sent_at IS NOT NULL) AS sent_iterations
        FROM reconciliations r
        WHERE r.recipient_email = ? COLLATE NOCASE
        ORDER BY r.id DESC
        """,
        (em,),
    ).fetchall()
    result: list[dict[str, Any]] = []
    for row in rows:
        d = dict(row)
        sent_iters = int(d.get("sent_iterations") or 0)
        pending = int(d.get("pending_count") or 0)
        if sent_iters == 0:
            iteration = 1
        elif pending > 0:
            iteration = sent_iters + 1
        else:
            iteration = sent_iters
        d["iteration"] = iteration
        d["sent_iterations"] = sent_iters
        result.append(d)
    return result

def confirmation_lines_for_reconciliation(
    conn: sqlite3.Connection, reconciliation_id: int
) -> list[str]:
    """Plain-text-список строк подтверждений (fallback в письме)."""
    rows = confirmation_rows_for_reconciliation(conn, reconciliation_id)
    return [
        format_confirmation_line(
            str(r["id_yavleniya"]),
            str(r["id_sopostavlennyi"]),
            (str(r["event_date"]) if r["event_date"] else None),
        )
        for r in rows
    ]

def confirmation_rows_for_reconciliation(
    conn: sqlite3.Connection, reconciliation_id: int
) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT id_yavleniya, id_sopostavlennyi, event_date,
               sent_at, inserted_at, digest_sent_at
        FROM confirmations
        WHERE reconciliation_id = ? ORDER BY id
        """,
        (reconciliation_id,),
    ).fetchall()

def delete_pending_from_reconciliation(
    conn: sqlite3.Connection,
    reconciliation_id: int,
    deletions: list[tuple[str, str, Optional[str]]],
) -> int:

    if not deletions:
        return 0

    total_removed = 0
    for id_yav, id_sop, event_date in deletions:
        params: list[object] = [reconciliation_id, str(id_yav), str(id_sop)]
        where_date = ""
        if event_date:
            where_date = " AND event_date = ?"
            params.append(event_date)
        cur = conn.execute(
            f"""
            DELETE FROM confirmations
            WHERE reconciliation_id = ?
              AND digest_sent_at IS NULL
              AND id_yavleniya = ?
              AND id_sopostavlennyi = ?{where_date}
            """,
            params,
        )
        total_removed += cur.rowcount or 0

    if total_removed:
        refresh_reconciliation_started_at(conn, reconciliation_id)

    return total_removed


def mark_reconciliation_sent(
    conn: sqlite3.Connection, reconciliation_id: int, when: str
) -> None:
    conn.execute(
        "UPDATE reconciliations SET sent_at = ? WHERE id = ?",
        (when, reconciliation_id),
    )
    conn.execute(
        """
        UPDATE confirmations SET digest_sent_at = ?
        WHERE reconciliation_id = ? AND digest_sent_at IS NULL
        """,
        (when, reconciliation_id),
    )

def backfill_reconciliations(conn: sqlite3.Connection) -> None:
    """Один раз: привязать старые письма к сверкам по digest_sent_at."""
    groups = conn.execute(
        """
        SELECT recipient_email, digest_sent_at AS sent_at,
               MIN(COALESCE(NULLIF(TRIM(sent_at), ''), inserted_at)) AS started_at
        FROM confirmations
        WHERE recipient_email IS NOT NULL AND recipient_email != ''
              AND digest_sent_at IS NOT NULL AND reconciliation_id IS NULL
        GROUP BY recipient_email, digest_sent_at
        """
    ).fetchall()
    for g in groups:
        cur = conn.execute(
            """
            INSERT INTO reconciliations (recipient_email, started_at, sent_at)
            VALUES (?, ?, ?)
            """,
            (g["recipient_email"], g["started_at"], g["sent_at"]),
        )
        rid = int(cur.lastrowid)
        conn.execute(
            """
            UPDATE confirmations SET reconciliation_id = ?
            WHERE recipient_email = ? COLLATE NOCASE
              AND digest_sent_at = ? AND reconciliation_id IS NULL
            """,
            (rid, g["recipient_email"], g["sent_at"]),
        )
    pending = conn.execute(
        """
        SELECT DISTINCT recipient_email AS em FROM confirmations
        WHERE digest_sent_at IS NULL AND recipient_email IS NOT NULL
              AND reconciliation_id IS NULL
        """
    ).fetchall()
    for row in pending:
        link_orphan_pending_to_open(conn, str(row["em"]))
    conn.commit()
