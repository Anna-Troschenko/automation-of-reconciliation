from __future__ import annotations

import sqlite3
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Callable, Iterable, Optional

_LABEL_ESCAPES = str.maketrans(
    {
        "\\": "\\\\",
        '"': '\\"',
        "\n": "\\n",
    }
)


def _escape_label_value(value: str) -> str:
    return value.translate(_LABEL_ESCAPES)


def _format_labels(label_pairs: tuple[tuple[str, str], ...]) -> str:
    if not label_pairs:
        return ""
    return (
        "{"
        + ",".join(f'{k}="{_escape_label_value(v)}"' for k, v in label_pairs)
        + "}"
    )


class _MetricFamily:
    metric_type: str = "untyped"

    def __init__(
        self,
        name: str,
        help_text: str,
        labelnames: Iterable[str] = (),
        *,
        registry: Optional["MetricsRegistry"] = None,
    ) -> None:
        self.name = name
        self.help_text = help_text
        self.labelnames: tuple[str, ...] = tuple(labelnames)
        self._values: dict[tuple[tuple[str, str], ...], float] = {}
        self._lock = threading.Lock()
        target = registry if registry is not None else REGISTRY
        target.register(self)

    def _key(self, **labels: str) -> tuple[tuple[str, str], ...]:
        if set(labels) != set(self.labelnames):
            raise ValueError(
                f"Метрика {self.name!r}: ожидаются лейблы {self.labelnames!r}, "
                f"переданы {tuple(labels)!r}"
            )
        return tuple((name, str(labels[name])) for name in self.labelnames)

    def _samples(self) -> list[tuple[tuple[tuple[str, str], ...], float]]:
        with self._lock:
            return sorted(self._values.items())


class _CounterChild:
    __slots__ = ("_parent", "_key")

    def __init__(
        self, parent: "Counter", key: tuple[tuple[str, str], ...]
    ) -> None:
        self._parent = parent
        self._key = key

    def inc(self, amount: float = 1.0) -> None:
        if amount < 0:
            raise ValueError("Counter не может уменьшаться")
        with self._parent._lock:
            self._parent._values[self._key] = (
                self._parent._values.get(self._key, 0.0) + float(amount)
            )


class Counter(_MetricFamily):
    metric_type = "counter"

    def labels(self, **labels: str) -> _CounterChild:
        key = self._key(**labels)
        with self._lock:
            self._values.setdefault(key, 0.0)
        return _CounterChild(self, key)

    def inc(self, amount: float = 1.0) -> None:
        """Шорткат для метрик без лейблов."""
        if self.labelnames:
            raise ValueError(
                f"Counter {self.name!r} имеет лейблы, используйте .labels(...)"
            )
        self.labels().inc(amount)


class _GaugeChild:
    __slots__ = ("_parent", "_key")

    def __init__(self, parent: "Gauge", key: tuple[tuple[str, str], ...]) -> None:
        self._parent = parent
        self._key = key

    def set(self, value: float) -> None:
        with self._parent._lock:
            self._parent._values[self._key] = float(value)

    def inc(self, amount: float = 1.0) -> None:
        with self._parent._lock:
            self._parent._values[self._key] = (
                self._parent._values.get(self._key, 0.0) + float(amount)
            )

    def dec(self, amount: float = 1.0) -> None:
        self.inc(-amount)


class Gauge(_MetricFamily):
    metric_type = "gauge"

    def labels(self, **labels: str) -> _GaugeChild:
        key = self._key(**labels)
        with self._lock:
            self._values.setdefault(key, 0.0)
        return _GaugeChild(self, key)

    def set(self, value: float) -> None:
        if self.labelnames:
            raise ValueError(
                f"Gauge {self.name!r} имеет лейблы, используйте .labels(...).set"
            )
        self.labels().set(value)


class MetricsRegistry:
    def __init__(self) -> None:
        self._metrics: dict[str, _MetricFamily] = {}
        self._collect_hooks: list[Callable[[], None]] = []
        self._lock = threading.Lock()

    def register(self, metric: _MetricFamily) -> None:
        with self._lock:
            if metric.name in self._metrics:
                return
            self._metrics[metric.name] = metric

    def add_collect_hook(self, fn: Callable[[], None]) -> None:
        with self._lock:
            self._collect_hooks.append(fn)

    def render(self) -> bytes:
        for hook in list(self._collect_hooks):
            try:
                hook()
            except Exception:
                pass
        out: list[str] = []
        with self._lock:
            metrics = list(self._metrics.values())
        for m in metrics:
            out.append(f"# HELP {m.name} {m.help_text}")
            out.append(f"# TYPE {m.name} {m.metric_type}")
            for label_pairs, value in m._samples():
                out.append(f"{m.name}{_format_labels(label_pairs)} {value:g}")
        out.append("")
        return "\n".join(out).encode("utf-8")


REGISTRY = MetricsRegistry()

PROCESS_START_TIME = Gauge(
    "mail_confirm_process_start_time_seconds",
    "Unix-время старта процесса.",
)
PROCESS_START_TIME.set(time.time())

CONFIRMATIONS_INSERTED = Counter(
    "mail_confirm_confirmations_inserted_total",
    "Сколько новых строк подтверждений записано в БД.",
)

CONFIRMATIONS_SKIPPED = Counter(
    "mail_confirm_confirmations_skipped_total",
    "Сколько подтверждений было пропущено при сканировании (дубликаты, "
    "пауза, неизвестный получатель и т.п.).",
    labelnames=("reason",),
)

DIGESTS_SENT = Counter(
    "mail_confirm_digests_sent_total",
    "Сколько сводок успешно ушло по SMTP.",
    labelnames=("kind",),  # initial / supplement
)

DIGEST_LETTERS_SENT = Counter(
    "mail_confirm_digest_letters_sent_total",
    "Сколько отдельных строк (подтверждений) ушло внутри сводок.",
    labelnames=("kind",),
)

SMTP_ERRORS = Counter(
    "mail_confirm_smtp_errors_total",
    "Ошибки SMTP при попытке отправить сводку.",
)

IMAP_SCANS = Counter(
    "mail_confirm_imap_scans_total",
    "Сколько раз демон сходил в IMAP за новыми письмами.",
)

IMAP_ERRORS = Counter(
    "mail_confirm_imap_errors_total",
    "Ошибки IMAP во время сканирования.",
)

WEB_REQUESTS = Counter(
    "mail_confirm_web_requests_total",
    "HTTP-запросы к веб-интерфейсу.",
    labelnames=("method", "endpoint", "status"),
)

PENDING_CONFIRMATIONS = Gauge(
    "mail_confirm_pending_confirmations",
    "Сколько подтверждений сейчас в очереди (digest_sent_at IS NULL).",
)

OPEN_RECONCILIATIONS = Gauge(
    "mail_confirm_open_reconciliations",
    "Сколько открытых сверок (sent_at IS NULL).",
)

RECIPIENTS_TOTAL = Gauge(
    "mail_confirm_recipients_total",
    "Сколько настроенных получателей в БД (по статусу).",
    labelnames=("status",),
)

for _kind in ("initial", "supplement"):
    DIGESTS_SENT.labels(kind=_kind)
    DIGEST_LETTERS_SENT.labels(kind=_kind)
for _reason in (
    "duplicate",
    "paused",
    "blocked",
    "unconfigured",
    "no_recipient",
    "pause_backlog",
):
    CONFIRMATIONS_SKIPPED.labels(reason=_reason)

def install_db_collector(db_path: str) -> None:

    def _collect() -> None:
        try:
            conn = sqlite3.connect(db_path, timeout=2.0)
        except sqlite3.Error:
            return
        conn.row_factory = sqlite3.Row
        try:
            row = conn.execute(
                "SELECT COUNT(*) AS c FROM confirmations WHERE digest_sent_at IS NULL"
            ).fetchone()
            PENDING_CONFIRMATIONS.set(int(row["c"]) if row else 0)

            row = conn.execute(
                "SELECT COUNT(*) AS c FROM reconciliations WHERE sent_at IS NULL"
            ).fetchone()
            OPEN_RECONCILIATIONS.set(int(row["c"]) if row else 0)

            rows = conn.execute(
                """
                SELECT COALESCE(NULLIF(status, ''), 'active') AS s, COUNT(*) AS c
                FROM recipient_digest
                WHERE email NOT IN (SELECT email FROM recipient_blocklist)
                GROUP BY s
                """
            ).fetchall()
            for status in ("active", "paused"):
                RECIPIENTS_TOTAL.labels(status=status).set(0)
            for r in rows:
                RECIPIENTS_TOTAL.labels(status=str(r["s"])).set(int(r["c"]))
        except sqlite3.Error:
            return
        finally:
            conn.close()

    REGISTRY.add_collect_hook(_collect)


class _MetricsHandler(BaseHTTPRequestHandler):
    def log_message(self, format: str, *args) -> None:
        pass

    def do_GET(self) -> None:  # noqa: N802 — имя метода диктует BaseHTTPRequestHandler
        if self.path.rstrip("/") in ("/metrics", ""):
            body = REGISTRY.render()
            self.send_response(200)
            self.send_header(
                "Content-Type", "text/plain; version=0.0.4; charset=utf-8"
            )
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if self.path.rstrip("/") == "/health":
            body = b'{"ok":true}'
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        self.send_error(404)


def start_http_server(
    host: str = "0.0.0.0",
    port: int = 9101,
    *,
    db_path: Optional[str] = None,
) -> ThreadingHTTPServer:

    if db_path:
        install_db_collector(db_path)

    server = ThreadingHTTPServer((host, port), _MetricsHandler)
    thread = threading.Thread(
        target=server.serve_forever,
        name="metrics-http",
        daemon=True,
    )
    thread.start()
    print(
        f"metrics: HTTP /metrics слушает на http://{host}:{port}/metrics",
        file=sys.stderr,
    )
    return server
