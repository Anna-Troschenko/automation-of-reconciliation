from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from typing import Any, Optional

from mail_confirm.triggers import (
    TRIGGER_INTERVAL,
    TRIGGER_KEYWORD,
    VALID_TRIGGER_TYPES,
    interval_seconds_from_trigger,
    load_recipient_trigger,
)
from mail_confirm.utils import parse_sql_datetime, parse_stored_sent_at, utc_now_sql

RECIPIENT_STATUS_ACTIVE = "active"
RECIPIENT_STATUS_PAUSED = "paused"

def normalize_recipient_email(email: str) -> str:
    return email.strip().lower()

def open_database_fast(db: str) -> sqlite3.Connection:
    """Быстрый коннект для горячего пути (HTTP-запросы веба).
    Не выполняет миграции, индексы и purge — это уже сделано один раз
    при старте через open_database(). PRAGMA выставлены для скорости."""
    if db.startswith(("postgresql://", "postgres://")):
        raise RuntimeError(
            "Поддерживается только SQLite: укажите путь к файлу .db "
            "(флаг --db или MAIL_DB / GMAIL_DB)."
        )
    conn = sqlite3.connect(db, timeout=5.0)
    conn.row_factory = sqlite3.Row

    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA temp_store=MEMORY")
    return conn

def open_database(db: str) -> sqlite3.Connection:
    if db.startswith(("postgresql://", "postgres://")):
        raise RuntimeError(
            "Поддерживается только SQLite: укажите путь к файлу .db "
            "(флаг --db или MAIL_DB / GMAIL_DB)."
        )

    conn = sqlite3.connect(db, timeout=15.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA temp_store=MEMORY")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS confirmations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            gmail_message_id TEXT NOT NULL UNIQUE,
            id_yavleniya TEXT NOT NULL,
            id_sopostavlennyi TEXT NOT NULL,
            subject TEXT,
            sent_at TEXT,
            event_date TEXT,
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
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS recipient_blocklist (
            email TEXT PRIMARY KEY COLLATE NOCASE
        )
        """
    )
    _migrate_confirmations(conn)
    _migrate_recipient_digest(conn)
    _migrate_reconciliations(conn)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_confirmations_ids "
        "ON confirmations (id_yavleniya, id_sopostavlennyi)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_confirmations_recipient_pending "
        "ON confirmations (recipient_email) WHERE digest_sent_at IS NULL"
    )
    purge_blocked_recipient_rows(conn)
    conn.commit()
    return conn

def purge_blocked_recipient_rows(conn: sqlite3.Connection) -> int:
    cur = conn.execute(
        """
        DELETE FROM recipient_digest
        WHERE email IN (SELECT email FROM recipient_blocklist)
        """
    )
    return int(cur.rowcount or 0)

def sync_database_reads(conn: sqlite3.Connection) -> None:
    """Подтянуть коммиты из других процессов (веб-UI) и убрать «призрачные» строки."""
    conn.commit()
    purge_blocked_recipient_rows(conn)
    conn.commit()

def _migrate_confirmations(conn: sqlite3.Connection) -> None:
    cur = conn.execute("PRAGMA table_info(confirmations)")
    cols = {row[1] for row in cur.fetchall()}
    if "recipient_email" not in cols:
        conn.execute("ALTER TABLE confirmations ADD COLUMN recipient_email TEXT")
    if "digest_sent_at" not in cols:
        conn.execute("ALTER TABLE confirmations ADD COLUMN digest_sent_at TEXT")
    if "event_date" not in cols:

        conn.execute("ALTER TABLE confirmations ADD COLUMN event_date TEXT")

def _migrate_recipient_digest(conn: sqlite3.Connection) -> None:
    cur = conn.execute("PRAGMA table_info(recipient_digest)")
    cols = {row[1] for row in cur.fetchall()}
    if "trigger_type" not in cols:
        conn.execute(
            "ALTER TABLE recipient_digest ADD COLUMN trigger_type TEXT NOT NULL DEFAULT 'interval'"
        )
    if "trigger_config" not in cols:
        conn.execute(
            "ALTER TABLE recipient_digest ADD COLUMN trigger_config TEXT NOT NULL DEFAULT '{}'"
        )
    if "immediate_digest" not in cols:
        conn.execute(
            "ALTER TABLE recipient_digest ADD COLUMN immediate_digest INTEGER NOT NULL DEFAULT 0"
        )
    if "status" not in cols:
        conn.execute(
            "ALTER TABLE recipient_digest ADD COLUMN status TEXT NOT NULL DEFAULT 'active'"
        )
    if "accumulation_resumed_at" not in cols:
        conn.execute(
            "ALTER TABLE recipient_digest ADD COLUMN accumulation_resumed_at TEXT"
        )
    conn.execute(
        """
        UPDATE recipient_digest
        SET trigger_config = json_object('interval_seconds', interval_seconds)
        WHERE (trigger_config IS NULL OR trigger_config = '' OR trigger_config = '{}')
          AND trigger_type = 'interval'
        """
    )
    conn.commit()

def _migrate_reconciliations(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS reconciliations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            recipient_email TEXT NOT NULL,
            started_at TEXT NOT NULL,
            sent_at TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_reconciliations_recipient
        ON reconciliations (recipient_email)
        """
    )
    cur = conn.execute("PRAGMA table_info(confirmations)")
    cols = {row[1] for row in cur.fetchall()}
    if "reconciliation_id" not in cols:
        conn.execute("ALTER TABLE confirmations ADD COLUMN reconciliation_id INTEGER")
    cur = conn.execute("PRAGMA table_info(recipient_digest)")
    rd_cols = {row[1] for row in cur.fetchall()}
    if "company_name" not in rd_cols:
        conn.execute("ALTER TABLE recipient_digest ADD COLUMN company_name TEXT NOT NULL DEFAULT ''")
    conn.commit()
    from mail_confirm.reconciliations import backfill_reconciliations

    key = "reconciliations_backfill_v1"
    done = conn.execute("SELECT 1 FROM daemon_meta WHERE key = ?", (key,)).fetchone()
    if not done:
        backfill_reconciliations(conn)
        conn.execute(
            "INSERT INTO daemon_meta (key, value) VALUES (?, ?)", (key, "1")
        )
        conn.commit()

def is_recipient_blocked(conn: sqlite3.Connection, email: str) -> bool:
    em = normalize_recipient_email(email)
    if not em:
        return False
    row = conn.execute(
        "SELECT 1 FROM recipient_blocklist WHERE email = ? COLLATE NOCASE",
        (em,),
    ).fetchone()
    return row is not None

def is_recipient_configured(conn: sqlite3.Connection, email: str) -> bool:
    em = normalize_recipient_email(email)
    if not em or is_recipient_blocked(conn, em):
        return False
    row = conn.execute(
        "SELECT 1 FROM recipient_digest WHERE email = ? COLLATE NOCASE",
        (em,),
    ).fetchone()
    return row is not None

def recipient_can_accumulate(conn: sqlite3.Connection, email: str) -> bool:
    em = normalize_recipient_email(email)
    if not em or not is_recipient_configured(conn, em):
        return False
    row = conn.execute(
        "SELECT status FROM recipient_digest WHERE email = ? COLLATE NOCASE",
        (em,),
    ).fetchone()
    if row is None:
        return False
    return str(row["status"] or RECIPIENT_STATUS_ACTIVE) == RECIPIENT_STATUS_ACTIVE

def get_accumulation_cutoff(conn: sqlite3.Connection, email: str) -> Optional[datetime]:
    em = normalize_recipient_email(email)
    if not em:
        return None
    row = conn.execute(
        """
        SELECT accumulation_resumed_at FROM recipient_digest
        WHERE email = ? COLLATE NOCASE
        """,
        (em,),
    ).fetchone()
    if row is None or not row["accumulation_resumed_at"]:
        return None
    try:
        return parse_sql_datetime(str(row["accumulation_resumed_at"]))
    except ValueError:
        return None

def confirmation_acceptable_for_recipient(
    conn: sqlite3.Connection, email: str, letter_sent_hdr: Optional[str]
) -> bool:
    """После снятия паузы / добавления компании не принимаем письма, отправленные раньше.

    Сравнивается дата отправки письма (заголовок Date), а не «Дата получения» из тела."""
    cutoff = get_accumulation_cutoff(conn, email)
    if cutoff is None:
        return True
    msg_dt = parse_stored_sent_at(letter_sent_hdr)
    if msg_dt is None:
        return False
    s = str(letter_sent_hdr or "").strip()
    if len(s) == 10 and s[4] == "-" and s[7] == "-":
        return msg_dt.date() >= cutoff.date()
    return msg_dt >= cutoff

def count_pending_for_recipient(conn: sqlite3.Connection, email: str) -> int:
    row = conn.execute(
        """
        SELECT COUNT(*) AS c FROM confirmations
        WHERE recipient_email = ? COLLATE NOCASE AND digest_sent_at IS NULL
        """,
        (email.strip().lower(),),
    ).fetchone()
    return int(row["c"]) if row else 0

def insert_confirmation_row(
    conn: sqlite3.Connection,
    dedupe: str,
    id_yav: str,
    id_sop: str,
    subj: str,
    date_hdr: Optional[str],
    recipient_email: str,
    *,
    append_reconciliation_id: Optional[int] = None,
    event_date: Optional[str] = None,
    received_date: Optional[str] = None,
) -> bool:
    em = normalize_recipient_email(recipient_email)
    if not em or not recipient_can_accumulate(conn, em):
        return False
    sent_at = received_date if received_date else date_hdr
    if not confirmation_acceptable_for_recipient(conn, em, date_hdr):
        return False
    try:
        cur = conn.execute(
            """
            INSERT INTO confirmations
            (gmail_message_id, id_yavleniya, id_sopostavlennyi, subject, sent_at,
             event_date, recipient_email, digest_sent_at, reconciliation_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL)
            """,
            (dedupe, str(id_yav), str(id_sop), subj, sent_at, event_date, em),
        )

        conf_id = int(cur.lastrowid)
        from mail_confirm.reconciliations import (
            attach_confirmation_to_reconciliation,
            get_or_create_open_reconciliation,
            reconciliation_belongs_to,
        )

        rid: Optional[int] = None
        if append_reconciliation_id is not None and reconciliation_belongs_to(
            conn, append_reconciliation_id, em
        ):
            rid = append_reconciliation_id
        if rid is None:
            rid = get_or_create_open_reconciliation(
                conn, em, letter_sent_hdr=sent_at
            )
        attach_confirmation_to_reconciliation(conn, conf_id, rid)
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        conn.rollback()
        return False

def ensure_recipient_digest_row(
    conn: sqlite3.Connection, email_norm: str, default_interval: int
) -> None:
    em = normalize_recipient_email(email_norm)
    if not em or is_recipient_blocked(conn, em):
        return
    conn.execute(
        """
        INSERT OR IGNORE INTO recipient_digest (
            email, interval_seconds, last_digest_sent_at,
            trigger_type, trigger_config, immediate_digest, status
        )
        VALUES (?, ?, NULL, 'interval', ?, 0, ?)
        """,
        (
            em,
            default_interval,
            json.dumps({"interval_seconds": default_interval}, ensure_ascii=False),
            RECIPIENT_STATUS_ACTIVE,
        ),
    )
    conn.commit()

def touch_last_digest_sent(conn: sqlite3.Connection, email: str, when: str) -> None:
    em = normalize_recipient_email(email)
    if not em or is_recipient_blocked(conn, em):
        return
    conn.execute(
        """
        UPDATE recipient_digest SET last_digest_sent_at = ?
        WHERE email = ? COLLATE NOCASE
        """,
        (when, em),
    )

def set_recipient_interval(conn: sqlite3.Connection, email: str, interval_sec: int) -> None:
    set_recipient_trigger(
        conn,
        email,
        TRIGGER_INTERVAL,
        {"interval_seconds": interval_sec},
    )

def set_recipient_trigger(
    conn: sqlite3.Connection,
    email: str,
    trigger_type: str,
    trigger_config: dict[str, Any],
    *,
    company_name: str = "",
) -> None:
    if trigger_type not in VALID_TRIGGER_TYPES:
        raise ValueError(f"Неизвестный тип триггера: {trigger_type}")
    em = normalize_recipient_email(email)
    if not em:
        raise ValueError("Укажите корректный e-mail")
    cfg = dict(trigger_config)
    interval_sec = int(cfg.get("interval_seconds", 300))
    if trigger_type == TRIGGER_INTERVAL:
        interval_sec = int(cfg.get("interval_seconds", interval_sec))
    elif trigger_type == TRIGGER_KEYWORD and "interval_seconds" not in cfg:
        interval_sec = 300
    company = (company_name or "").strip()
    resumed_at = utc_now_sql()
    conn.execute("DELETE FROM recipient_blocklist WHERE email = ? COLLATE NOCASE", (em,))
    conn.execute(
        """
        INSERT INTO recipient_digest (
            email, interval_seconds, last_digest_sent_at,
            trigger_type, trigger_config, immediate_digest, status, company_name,
            accumulation_resumed_at
        )
        VALUES (?, ?, NULL, ?, ?, 0, ?, ?, ?)
        ON CONFLICT(email) DO UPDATE SET
            interval_seconds = excluded.interval_seconds,
            trigger_type = excluded.trigger_type,
            trigger_config = excluded.trigger_config,
            immediate_digest = 0,
            status = excluded.status,
            company_name = excluded.company_name
        """,
        (
            em,
            interval_sec,
            trigger_type,
            json.dumps(cfg, ensure_ascii=False),
            RECIPIENT_STATUS_ACTIVE,
            company,
            resumed_at,
        ),
    )
    conn.commit()

def get_recipient_interval(conn: sqlite3.Connection, email: str, default_interval: int) -> int:
    rt = load_recipient_trigger(conn, email, default_interval)
    if rt is None:
        return default_interval
    return interval_seconds_from_trigger(rt, default_interval)

def list_recipient_triggers(
    conn: sqlite3.Connection, *, search: str = ""
) -> list[sqlite3.Row]:
    rows = conn.execute(
        """
        SELECT rd.email, rd.company_name, rd.trigger_type, rd.trigger_config,
               rd.interval_seconds, rd.immediate_digest, rd.last_digest_sent_at, rd.status,
               (SELECT COUNT(*) FROM confirmations c
                WHERE c.recipient_email = rd.email COLLATE NOCASE
                  AND c.digest_sent_at IS NULL) AS pending_count,
               (SELECT id FROM reconciliations r
                WHERE r.recipient_email = rd.email COLLATE NOCASE
                  AND r.sent_at IS NULL ORDER BY r.id DESC LIMIT 1) AS open_reconciliation_id
        FROM recipient_digest rd
        WHERE rd.email NOT IN (SELECT email FROM recipient_blocklist)
        ORDER BY rd.company_name COLLATE NOCASE, rd.email COLLATE NOCASE
        """
    ).fetchall()
    q = (search or "").strip().casefold()
    if not q:
        return rows

    out: list[sqlite3.Row] = []
    for r in rows:
        email = str(r["email"] or "").casefold()
        name = str(r["company_name"] or "").casefold()
        if q in email or q in name:
            out.append(r)
    return out

def get_recipient_profile(conn: sqlite3.Connection, email: str) -> Optional[sqlite3.Row]:
    em = normalize_recipient_email(email)
    if is_recipient_blocked(conn, em):
        return None
    return conn.execute(
        """
        SELECT rd.email, rd.company_name, rd.trigger_type, rd.trigger_config,
               rd.interval_seconds, rd.immediate_digest, rd.last_digest_sent_at, rd.status,
               (SELECT COUNT(*) FROM confirmations c
                WHERE c.recipient_email = rd.email COLLATE NOCASE
                  AND c.digest_sent_at IS NULL) AS pending_count,
               (SELECT id FROM reconciliations r
                WHERE r.recipient_email = rd.email COLLATE NOCASE
                  AND r.sent_at IS NULL ORDER BY r.id DESC LIMIT 1) AS open_reconciliation_id
        FROM recipient_digest rd
        WHERE rd.email = ? COLLATE NOCASE
        """,
        (em,),
    ).fetchone()

def pause_recipient(conn: sqlite3.Connection, email: str) -> None:
    em = normalize_recipient_email(email)
    if is_recipient_blocked(conn, em):
        raise ValueError("Получатель удалён — сначала сохраните настройки заново")
    row = conn.execute(
        "SELECT 1 FROM recipient_digest WHERE email = ? COLLATE NOCASE", (em,)
    ).fetchone()
    if row is None:
        raise ValueError("Получатель не найден — сначала сохраните настройки")
    conn.execute(
        "UPDATE recipient_digest SET status = ? WHERE email = ? COLLATE NOCASE",
        (RECIPIENT_STATUS_PAUSED, em),
    )
    conn.commit()

def resume_recipient(conn: sqlite3.Connection, email: str) -> None:
    em = normalize_recipient_email(email)
    if is_recipient_blocked(conn, em):
        raise ValueError("Получатель удалён — сначала сохраните настройки заново")
    row = conn.execute(
        "SELECT 1 FROM recipient_digest WHERE email = ? COLLATE NOCASE", (em,)
    ).fetchone()
    if row is None:
        raise ValueError("Получатель не найден — сначала сохраните настройки")
    resumed = utc_now_sql()
    conn.execute(
        """
        UPDATE recipient_digest
        SET status = ?, accumulation_resumed_at = ?
        WHERE email = ? COLLATE NOCASE
        """,
        (RECIPIENT_STATUS_ACTIVE, resumed, em),
    )
    conn.commit()

def delete_recipient(conn: sqlite3.Connection, email: str) -> int:
    em = normalize_recipient_email(email)
    if not em:
        raise ValueError("Укажите корректный e-mail")
    conn.execute(
        "INSERT OR IGNORE INTO recipient_blocklist (email) VALUES (?)", (em,)
    )
    deleted = conn.execute(
        """
        DELETE FROM confirmations
        WHERE recipient_email = ? COLLATE NOCASE
        """,
        (em,),
    ).rowcount
    conn.execute("DELETE FROM recipient_digest WHERE email = ? COLLATE NOCASE", (em,))
    conn.commit()
    return int(deleted or 0)

def request_immediate_digest(conn: sqlite3.Connection, email: str) -> int:
    em = email.strip().lower()
    if is_recipient_blocked(conn, em):
        raise ValueError("Получатель удалён")
    pending = count_pending_for_recipient(conn, em)
    if pending == 0:
        raise ValueError("Нет писем в очереди сводки для этого адреса")
    row = conn.execute(
        "SELECT 1 FROM recipient_digest WHERE email = ? COLLATE NOCASE", (em,)
    ).fetchone()
    if row is None:
        raise ValueError("Сначала сохраните настройки для этого получателя")
    conn.execute(
        "UPDATE recipient_digest SET immediate_digest = 1 WHERE email = ? COLLATE NOCASE",
        (em,),
    )
    conn.commit()
    return pending

def clear_immediate_digest(conn: sqlite3.Connection, email: str) -> None:
    conn.execute(
        "UPDATE recipient_digest SET immediate_digest = 0 WHERE email = ? COLLATE NOCASE",
        (email.lower(),),
    )
    conn.commit()

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

DEFAULT_DIGEST_INTRO_TEXT = (
    "Добрый день! Направляем вам сводку подтверждённых нежелательных явлений."
)


def get_digest_intro_text(conn: sqlite3.Connection) -> str:
    row = conn.execute(
        "SELECT value FROM daemon_meta WHERE key = ?", ("digest_intro_text",)
    ).fetchone()
    if row is None or row["value"] is None:
        return DEFAULT_DIGEST_INTRO_TEXT
    return str(row["value"])


def set_digest_intro_text(conn: sqlite3.Connection, text: str) -> None:
    conn.execute(
        """
        INSERT INTO daemon_meta (key, value) VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """,
        ("digest_intro_text", text),
    )
    conn.commit()


def collect_pending_recipients(conn: sqlite3.Connection) -> list[str]:

    rows = conn.execute(
        """
        SELECT DISTINCT c.recipient_email AS r FROM confirmations c
        WHERE c.digest_sent_at IS NULL AND c.recipient_email IS NOT NULL
              AND c.recipient_email != ''
              AND c.recipient_email NOT IN (SELECT email FROM recipient_blocklist)
        """
    ).fetchall()
    return [str(row["r"]) for row in rows]

def daemon_imap_idle_chunk_sec(
    conn: sqlite3.Connection, *, imap_cap: float, digest_default: int
) -> float:
    from mail_confirm.triggers import min_pending_wake_sec

    m = min_pending_wake_sec(conn, digest_default)
    if m is None:
        return min(float(imap_cap), float(digest_default))
    urgent = max(5.0, float(min(digest_default, m)))
    return min(float(imap_cap), urgent)

def daemon_poll_sec(conn: sqlite3.Connection, *, poll_cap: int, digest_default: int) -> int:
    from mail_confirm.triggers import min_pending_wake_sec

    m = min_pending_wake_sec(conn, digest_default)
    if m is None:
        return max(5, poll_cap)
    urgent = max(5, min(digest_default, m))
    return min(max(5, poll_cap), urgent)
