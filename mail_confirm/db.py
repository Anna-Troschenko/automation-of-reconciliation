from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Optional

from mail_confirm.utils import parse_sql_datetime, utc_now_sql


def open_database(db: str) -> sqlite3.Connection:
    if db.startswith(("postgresql://", "postgres://")):
        raise RuntimeError(
            "Поддерживается только SQLite: укажите путь к файлу .db "
            "(флаг --db или MAIL_DB / GMAIL_DB)."
        )

    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS confirmations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            gmail_message_id TEXT NOT NULL UNIQUE,
            id_yavleniya INTEGER NOT NULL,
            id_sopostavlennyi INTEGER NOT NULL,
            subject TEXT,
            sent_at TEXT,
            recipient_email TEXT,
            digest_sent_at TEXT,
            inserted_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS recipient_digest (
            email TEXT PRIMARY KEY COLLATE NOCASE,
            interval_seconds INTEGER NOT NULL DEFAULT 300,
            last_digest_sent_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS daemon_meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
        """
    )
    _migrate_confirmations(conn)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_confirmations_ids "
        "ON confirmations (id_yavleniya, id_sopostavlennyi)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_confirmations_recipient_pending "
        "ON confirmations (recipient_email) WHERE digest_sent_at IS NULL"
    )
    conn.commit()
    return conn


def _migrate_confirmations(conn: sqlite3.Connection) -> None:
    cur = conn.execute("PRAGMA table_info(confirmations)")
    cols = {row[1] for row in cur.fetchall()}
    if "recipient_email" not in cols:
        conn.execute("ALTER TABLE confirmations ADD COLUMN recipient_email TEXT")
    if "digest_sent_at" not in cols:
        conn.execute("ALTER TABLE confirmations ADD COLUMN digest_sent_at TEXT")


def insert_confirmation_row(
    conn: sqlite3.Connection,
    dedupe: str,
    id_yav: int,
    id_sop: int,
    subj: str,
    date_hdr: Optional[str],
    recipient_email: str,
) -> bool:
    try:
        conn.execute(
            """
            INSERT INTO confirmations
            (gmail_message_id, id_yavleniya, id_sopostavlennyi, subject, sent_at,
             recipient_email, digest_sent_at)
            VALUES (?, ?, ?, ?, ?, ?, NULL)
            """,
            (dedupe, id_yav, id_sop, subj, date_hdr, recipient_email or None),
        )
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        conn.rollback()
        return False


def ensure_recipient_digest_row(
    conn: sqlite3.Connection, email_norm: str, default_interval: int
) -> None:
    if not email_norm:
        return
    conn.execute(
        """
        INSERT OR IGNORE INTO recipient_digest (email, interval_seconds, last_digest_sent_at)
        VALUES (?, ?, NULL)
        """,
        (email_norm.lower(), default_interval),
    )
    conn.commit()


def set_recipient_interval(conn: sqlite3.Connection, email: str, interval_sec: int) -> None:
    em = email.strip().lower()
    conn.execute(
        """
        INSERT INTO recipient_digest (email, interval_seconds, last_digest_sent_at)
        VALUES (?, ?, NULL)
        ON CONFLICT(email) DO UPDATE SET interval_seconds = excluded.interval_seconds
        """,
        (em, interval_sec),
    )
    conn.commit()


def get_recipient_interval(conn: sqlite3.Connection, email: str, default_interval: int) -> int:
    row = conn.execute(
        "SELECT interval_seconds FROM recipient_digest WHERE email = ? COLLATE NOCASE",
        (email.lower(),),
    ).fetchone()
    if row is None:
        return default_interval
    return int(row["interval_seconds"])


def get_last_sent_imap_uid(conn: sqlite3.Connection, folder: str) -> int:
    key = f"sent_last_uid:{folder}"
    row = conn.execute(
        "SELECT value FROM daemon_meta WHERE key = ?", (key,)
    ).fetchone()
    if row is None:
        return 0
    try:
        return int(row["value"])
    except ValueError:
        return 0


def set_last_sent_imap_uid(conn: sqlite3.Connection, folder: str, uid: int) -> None:
    key = f"sent_last_uid:{folder}"
    conn.execute(
        """
        INSERT INTO daemon_meta (key, value) VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """,
        (key, str(uid)),
    )
    conn.commit()


def collect_pending_recipients(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        """
        SELECT DISTINCT recipient_email AS r FROM confirmations
        WHERE digest_sent_at IS NULL AND recipient_email IS NOT NULL AND recipient_email != ''
        """
    ).fetchall()
    return [str(row["r"]) for row in rows]


def min_pending_digest_interval_sec(
    conn: sqlite3.Connection, default_interval: int
) -> Optional[int]:
    row = conn.execute(
        """
        SELECT MIN(COALESCE(rd.interval_seconds, ?)) AS m
        FROM confirmations c
        LEFT JOIN recipient_digest rd
          ON rd.email = c.recipient_email COLLATE NOCASE
        WHERE c.digest_sent_at IS NULL AND c.recipient_email IS NOT NULL
              AND c.recipient_email != ''
        """,
        (default_interval,),
    ).fetchone()
    if row is None or row["m"] is None:
        return None
    return int(row["m"])


def daemon_imap_idle_chunk_sec(
    conn: sqlite3.Connection, *, imap_cap: float, digest_default: int
) -> float:
    m = min_pending_digest_interval_sec(conn, digest_default)
    if m is None:
        return min(float(imap_cap), float(digest_default))
    urgent = max(5.0, float(min(digest_default, m)))
    return min(float(imap_cap), urgent)


def daemon_poll_sec(conn: sqlite3.Connection, *, poll_cap: int, digest_default: int) -> int:
    m = min_pending_digest_interval_sec(conn, digest_default)
    if m is None:
        return max(5, poll_cap)
    urgent = max(5, min(digest_default, m))
    return min(max(5, poll_cap), urgent)


def digest_due(
    conn: sqlite3.Connection,
    recipient: str,
    default_interval: int,
    now: datetime,
) -> bool:
    row = conn.execute(
        """
        SELECT COUNT(*) AS c, MIN(inserted_at) AS first_ins
        FROM confirmations
        WHERE recipient_email = ? COLLATE NOCASE AND digest_sent_at IS NULL
        """,
        (recipient,),
    ).fetchone()
    if row is None or int(row["c"]) == 0:
        return False
    interval = get_recipient_interval(conn, recipient, default_interval)
    delta = timedelta(seconds=interval)
    last_row = conn.execute(
        "SELECT last_digest_sent_at FROM recipient_digest WHERE email = ? COLLATE NOCASE",
        (recipient,),
    ).fetchone()
    last_sent_s = last_row["last_digest_sent_at"] if last_row else None

    first_ins = row["first_ins"]
    first_dt: Optional[datetime] = None
    if first_ins:
        try:
            first_dt = parse_sql_datetime(str(first_ins))
            if first_dt.tzinfo is None:
                first_dt = first_dt.replace(tzinfo=timezone.utc)
        except ValueError:
            first_dt = None

    if last_sent_s:
        try:
            last_sent = parse_sql_datetime(str(last_sent_s))
            if last_sent.tzinfo is None:
                last_sent = last_sent.replace(tzinfo=timezone.utc)
        except ValueError:
            last_sent = now - delta
        start = last_sent
        if first_dt is not None and first_dt > last_sent:
            start = first_dt
        return now >= start + delta

    if not first_ins or first_dt is None:
        return False
    return now >= first_dt + delta
