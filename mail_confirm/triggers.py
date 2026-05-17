from __future__ import annotations

import calendar
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Optional
from zoneinfo import ZoneInfo

from mail_confirm.utils import parse_sql_datetime

TRIGGER_INTERVAL = "interval"
TRIGGER_SCHEDULE = "schedule"
TRIGGER_COUNT = "count"
TRIGGER_KEYWORD = "keyword"

VALID_TRIGGER_TYPES = frozenset(
    {TRIGGER_INTERVAL, TRIGGER_SCHEDULE, TRIGGER_COUNT, TRIGGER_KEYWORD}
)

DEFAULT_TZ = "Europe/Moscow"

@dataclass(frozen=True)
class RecipientTrigger:
    email: str
    trigger_type: str
    trigger_config: dict[str, Any]
    interval_seconds: int
    immediate_digest: bool
    last_digest_sent_at: Optional[str]

def _parse_config(raw: Optional[str], interval_seconds: int, trigger_type: str) -> dict[str, Any]:
    if raw:
        try:
            cfg = json.loads(raw)
            if isinstance(cfg, dict):
                return cfg
        except json.JSONDecodeError:
            pass
    if trigger_type == TRIGGER_INTERVAL:
        return {"interval_seconds": interval_seconds}
    return {}

def load_recipient_trigger(conn: sqlite3.Connection, email: str, default_interval: int) -> Optional[RecipientTrigger]:
    row = conn.execute(
        """
        SELECT email, trigger_type, trigger_config, interval_seconds,
               immediate_digest, last_digest_sent_at
        FROM recipient_digest WHERE email = ? COLLATE NOCASE
        """,
        (email.lower(),),
    ).fetchone()
    if row is None:
        return None
    tt = str(row["trigger_type"] or TRIGGER_INTERVAL)
    iv = int(row["interval_seconds"])
    return RecipientTrigger(
        email=str(row["email"]),
        trigger_type=tt,
        trigger_config=_parse_config(row["trigger_config"], iv, tt),
        interval_seconds=iv,
        immediate_digest=bool(int(row["immediate_digest"] or 0)),
        last_digest_sent_at=row["last_digest_sent_at"],
    )

def interval_seconds_from_trigger(rt: RecipientTrigger, default_interval: int) -> int:
    if rt.trigger_type == TRIGGER_INTERVAL:
        return int(rt.trigger_config.get("interval_seconds", rt.interval_seconds))
    return rt.interval_seconds or default_interval

def pending_count(conn: sqlite3.Connection, recipient: str) -> int:
    row = conn.execute(
        """
        SELECT COUNT(*) AS c FROM confirmations
        WHERE recipient_email = ? COLLATE NOCASE AND digest_sent_at IS NULL
        """,
        (recipient,),
    ).fetchone()
    return int(row["c"]) if row else 0

def _first_pending_inserted(conn: sqlite3.Connection, recipient: str) -> Optional[datetime]:
    row = conn.execute(
        """
        SELECT MIN(inserted_at) AS first_ins FROM confirmations
        WHERE recipient_email = ? COLLATE NOCASE AND digest_sent_at IS NULL
        """,
        (recipient,),
    ).fetchone()
    if not row or not row["first_ins"]:
        return None
    try:
        dt = parse_sql_datetime(str(row["first_ins"]))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return None

def _parse_last_sent(last_sent_s: Optional[str], now: datetime, delta: timedelta) -> Optional[datetime]:
    if not last_sent_s:
        return None
    try:
        last_sent = parse_sql_datetime(str(last_sent_s))
        if last_sent.tzinfo is None:
            last_sent = last_sent.replace(tzinfo=timezone.utc)
        return last_sent
    except ValueError:
        return now - delta

def _interval_due(
    rt: RecipientTrigger,
    *,
    first_dt: Optional[datetime],
    last_sent: Optional[datetime],
    now: datetime,
    default_interval: int,
) -> bool:
    interval = interval_seconds_from_trigger(rt, default_interval)
    delta = timedelta(seconds=interval)
    if last_sent:
        start = last_sent
        if first_dt is not None and first_dt > last_sent:
            start = first_dt
        return now >= start + delta
    if first_dt is None:
        return False
    return now >= first_dt + delta

def _schedule_slot(year: int, month: int, day: int, hour: int, minute: int, tz: ZoneInfo) -> datetime:
    last_dom = calendar.monthrange(year, month)[1]
    dom = min(day, last_dom)
    local = datetime(year, month, dom, hour, minute, 0, tzinfo=tz)
    return local.astimezone(timezone.utc)

def _add_months(year: int, month: int, delta: int) -> tuple[int, int]:
    month += delta
    while month > 12:
        month -= 12
        year += 1
    while month < 1:
        month += 12
        year -= 1
    return year, month

def last_schedule_fire_at_or_before(
    now: datetime, *, day: int, hour: int, minute: int, tz_name: str
) -> datetime:
    tz = ZoneInfo(tz_name)
    local = now.astimezone(tz)
    y, m = local.year, local.month
    slot = _schedule_slot(y, m, day, hour, minute, tz)
    if slot <= now:
        return slot
    y, m = _add_months(y, m, -1)
    return _schedule_slot(y, m, day, hour, minute, tz)

def next_schedule_fire_after(now: datetime, *, day: int, hour: int, minute: int, tz_name: str) -> datetime:
    tz = ZoneInfo(tz_name)
    local = now.astimezone(tz)
    y, m = local.year, local.month
    slot = _schedule_slot(y, m, day, hour, minute, tz)
    if slot > now:
        return slot
    y, m = _add_months(y, m, 1)
    return _schedule_slot(y, m, day, hour, minute, tz)

def _schedule_due(rt: RecipientTrigger, *, last_sent: Optional[datetime], now: datetime) -> bool:
    cfg = rt.trigger_config
    day = int(cfg.get("day_of_month", 1))
    hour = int(cfg.get("hour", 0))
    minute = int(cfg.get("minute", 0))
    tz_name = str(cfg.get("timezone", DEFAULT_TZ))
    slot = last_schedule_fire_at_or_before(now, day=day, hour=hour, minute=minute, tz_name=tz_name)
    if last_sent is None:
        return now >= slot
    return last_sent < slot

def _count_due(rt: RecipientTrigger, *, pending: int) -> bool:
    min_count = int(rt.trigger_config.get("min_count", 1))
    return pending >= max(1, min_count)

def digest_due_for_recipient(
    conn: sqlite3.Connection,
    recipient: str,
    default_interval: int,
    now: datetime,
) -> bool:
    from mail_confirm.db import is_recipient_blocked

    if is_recipient_blocked(conn, recipient):
        return False
    pending = pending_count(conn, recipient)
    if pending == 0:
        return False

    rt = load_recipient_trigger(conn, recipient, default_interval)
    if rt is None:
        return False

    if rt.immediate_digest:
        return True

    first_dt = _first_pending_inserted(conn, recipient)
    interval = interval_seconds_from_trigger(rt, default_interval)
    last_sent = _parse_last_sent(rt.last_digest_sent_at, now, timedelta(seconds=interval))

    if rt.trigger_type == TRIGGER_COUNT:
        return _count_due(rt, pending=pending)
    if rt.trigger_type == TRIGGER_SCHEDULE:
        return _schedule_due(rt, last_sent=last_sent, now=now)
    if rt.trigger_type == TRIGGER_KEYWORD:
        return False
    return _interval_due(
        rt, first_dt=first_dt, last_sent=last_sent, now=now, default_interval=default_interval
    )

def keyword_phrase(rt: RecipientTrigger) -> str:
    return str(rt.trigger_config.get("phrase", "")).strip()

def body_matches_keyword(body: str, phrase: str, *, case_sensitive: bool) -> bool:
    if not phrase:
        return False
    if case_sensitive:
        return phrase in body
    return phrase.lower() in body.lower()

def check_outbound_keyword(
    conn: sqlite3.Connection,
    recipient: str,
    body: str,
    default_interval: int,
) -> bool:
    from mail_confirm.db import is_recipient_blocked

    if is_recipient_blocked(conn, recipient):
        return False
    rt = load_recipient_trigger(conn, recipient, default_interval)
    if rt is None or rt.trigger_type != TRIGGER_KEYWORD:
        return False
    phrase = keyword_phrase(rt)
    if not phrase:
        return False
    case_sensitive = bool(rt.trigger_config.get("case_sensitive", False))
    if not body_matches_keyword(body, phrase, case_sensitive=case_sensitive):
        return False
    conn.execute(
        "UPDATE recipient_digest SET immediate_digest = 1 WHERE email = ? COLLATE NOCASE",
        (recipient.lower(),),
    )
    conn.commit()
    return True

def wake_seconds_for_recipient(
    conn: sqlite3.Connection,
    recipient: str,
    default_interval: int,
    now: datetime,
) -> Optional[int]:
    if pending_count(conn, recipient) == 0:
        return None
    rt = load_recipient_trigger(conn, recipient, default_interval)
    if rt is None:
        return None
    if rt.immediate_digest:
        return 1
    if rt.trigger_type == TRIGGER_COUNT:
        min_count = int(rt.trigger_config.get("min_count", 1))
        pending = pending_count(conn, recipient)
        if pending >= min_count:
            return 1
        return 30
    if rt.trigger_type == TRIGGER_SCHEDULE:
        cfg = rt.trigger_config
        nxt = next_schedule_fire_after(
            now,
            day=int(cfg.get("day_of_month", 1)),
            hour=int(cfg.get("hour", 0)),
            minute=int(cfg.get("minute", 0)),
            tz_name=str(cfg.get("timezone", DEFAULT_TZ)),
        )
        if _schedule_due(rt, last_sent=_parse_last_sent(rt.last_digest_sent_at, now, timedelta(0)), now=now):
            return 1
        return max(5, int((nxt - now).total_seconds()))
    if rt.trigger_type == TRIGGER_KEYWORD:
        return 60
    iv = interval_seconds_from_trigger(rt, default_interval)
    last_sent = _parse_last_sent(rt.last_digest_sent_at, now, timedelta(seconds=iv))
    first_dt = _first_pending_inserted(conn, recipient)
    if _interval_due(
        rt,
        first_dt=first_dt,
        last_sent=last_sent,
        now=now,
        default_interval=default_interval,
    ):
        return 1
    if last_sent:
        start = last_sent
        if first_dt and first_dt > last_sent:
            start = first_dt
        due_at = start + timedelta(seconds=iv)
    elif first_dt:
        due_at = first_dt + timedelta(seconds=iv)
    else:
        return iv
    return max(5, int((due_at - now).total_seconds()))

def min_pending_wake_sec(conn: sqlite3.Connection, default_interval: int) -> Optional[int]:
    now = datetime.now(timezone.utc)
    rows = conn.execute(
        """
        SELECT DISTINCT recipient_email AS r FROM confirmations
        WHERE digest_sent_at IS NULL AND recipient_email IS NOT NULL AND recipient_email != ''
        """
    ).fetchall()
    if not rows:
        return None
    secs = [
        s
        for row in rows
        if (s := wake_seconds_for_recipient(conn, str(row["r"]), default_interval, now)) is not None
    ]
    if not secs:
        return None
    return max(5, min(secs))
