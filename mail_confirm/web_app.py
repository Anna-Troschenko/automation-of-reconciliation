from __future__ import annotations

import hmac
import http.cookies
import json
import re
import secrets
import sqlite3
import sys
import threading
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Optional
from urllib.parse import parse_qs, quote, unquote, urlparse

from mail_confirm.db import (
    DEFAULT_DIGEST_INTRO_TEXT,
    RECIPIENT_STATUS_ACTIVE,
    RECIPIENT_STATUS_PAUSED,
    delete_recipient,
    get_digest_intro_text,
    get_recipient_profile,
    list_recipient_triggers,
    normalize_recipient_email,
    open_database,
    open_database_fast,
    pause_recipient,
    purge_blocked_recipient_rows,
    request_immediate_digest,
    resume_recipient,
    set_digest_intro_text,
    set_recipient_trigger,
)


from mail_confirm.reconciliations import (
    confirmation_rows_for_reconciliation,
    get_reconciliation,
    list_reconciliations_for_recipient,
)

from mail_confirm.metrics import REGISTRY as METRICS_REGISTRY, WEB_REQUESTS, install_db_collector
from mail_confirm.smtp_ops import SmtpConfig, send_reconciliation_by_id, smtp_config_from_env
from mail_confirm.triggers import TRIGGER_KEYWORD, VALID_TRIGGER_TYPES, _parse_config

_STATIC = Path(__file__).resolve().parent / "web_static"
_EMAIL_PATH = re.compile(r"^/api/companies/([^/]+)(?:/(reconciliations))?$")
_RECON_SEND = re.compile(r"^/api/reconciliations/(\d+)/send$")
_RECON_ROWS = re.compile(r"^/api/reconciliations/(\d+)/rows$")
_RECON_GET = re.compile(r"^/api/reconciliations/(\d+)$")

_UNAUTHENTICATED_PATHS: frozenset[str] = frozenset(
    {"/metrics", "/api/health", "/login", "/logout"}
)

_SESSION_COOKIE = "mc_session"
_SESSION_TTL_SEC = 7 * 24 * 3600

def _row_to_api(row: sqlite3.Row) -> dict[str, Any]:
    tt = str(row["trigger_type"] or "interval")
    iv = int(row["interval_seconds"])
    cfg = _parse_config(row["trigger_config"], iv, tt)
    status = str(row["status"] or RECIPIENT_STATUS_ACTIVE)
    open_rid = row["open_reconciliation_id"]
    return {
        "email": str(row["email"]),
        "company_name": str(row["company_name"] or ""),
        "trigger_type": tt,
        "trigger_config": cfg,
        "interval_seconds": iv,
        "immediate_digest": bool(int(row["immediate_digest"] or 0)),
        "last_digest_sent_at": row["last_digest_sent_at"],
        "status": status,
        "pending_count": int(row["pending_count"] or 0),
        "open_reconciliation_id": int(open_rid) if open_rid is not None else None,
    }

def _validate_payload(data: dict[str, Any]) -> tuple[str, str, dict[str, Any], str]:
    email = str(data.get("email", "")).strip().lower()
    if "@" not in email:
        raise ValueError("Укажите корректный e-mail")
    trigger_type = str(data.get("trigger_type", "")).strip()
    if trigger_type not in VALID_TRIGGER_TYPES:
        raise ValueError("Неизвестный тип триггера")
    company_name = str(data.get("company_name", "")).strip()
    cfg = dict(data.get("trigger_config") or {})
    if trigger_type == "interval":
        sec = int(cfg.get("interval_seconds", 86400))
        if sec < 86400:
            raise ValueError("Интервал не менее 1 дня")
        cfg = {"interval_seconds": sec}
    elif trigger_type == "schedule":
        day = int(cfg.get("day_of_month", 1))
        hour = int(cfg.get("hour", 0))
        minute = int(cfg.get("minute", 0))
        if not 1 <= day <= 31:
            raise ValueError("День месяца: от 1 до 31")
        if not 0 <= hour <= 23 or not 0 <= minute <= 59:
            raise ValueError("Некорректное время")
        tz = str(cfg.get("timezone", "Europe/Moscow")).strip() or "Europe/Moscow"
        cfg = {"day_of_month": day, "hour": hour, "minute": minute, "timezone": tz}
    elif trigger_type == "count":
        n = int(cfg.get("min_count", 1))
        if n < 1:
            raise ValueError("Порог не менее 1")
        cfg = {"min_count": n}
    elif trigger_type == TRIGGER_KEYWORD:
        phrase = str(cfg.get("phrase", "")).strip()
        if not phrase:
            raise ValueError("Укажите кодовое слово")
        cfg = {"phrase": phrase, "case_sensitive": bool(cfg.get("case_sensitive", False))}
    return email, trigger_type, cfg, company_name

def _bg_send_reconciliation(
    db_path: str, reconciliation_id: int, smtp: SmtpConfig
) -> None:
    conn = open_database_fast(db_path)
    try:
        n = send_reconciliation_by_id(conn, reconciliation_id, smtp)
        print(
            f"WEB: фоновая отправка сводки #{reconciliation_id} ок ({n} писем)",
            file=sys.stderr,
        )
    except Exception as e:
        print(
            f"WEB: ошибка фоновой отправки сводки #{reconciliation_id}: {e}",
            file=sys.stderr,
        )
    finally:
        conn.close()

def _metrics_endpoint(path: str) -> str:
    if path in ("/", "/index.html"):
        return "/"
    if path.startswith("/static/"):
        return "/static/*"
    if path.startswith("/api/companies/") and path.endswith("/reconciliations"):
        return "/api/companies/:email/reconciliations"
    if path.startswith("/api/companies/"):
        return "/api/companies/:email"
    if path.startswith("/api/reconciliations/") and path.endswith("/send"):
        return "/api/reconciliations/:id/send"
    if path.startswith("/api/reconciliations/") and path.endswith("/rows"):
        return "/api/reconciliations/:id/rows"
    if path.startswith("/api/reconciliations/"):
        return "/api/reconciliations/:id"
    if path == "/metrics":
        return "/metrics"
    if path.startswith("/api/"):
        return path
    return "other"


def _row_to_recon_row_api(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "id_yavleniya": str(row["id_yavleniya"] or ""),
        "id_sopostavlennyi": str(row["id_sopostavlennyi"] or ""),
        "event_date": row["event_date"],
        "sent_at": row["sent_at"],
        "inserted_at": row["inserted_at"],
        "digest_sent_at": row["digest_sent_at"],
    }



def make_handler(
    db_path: str,
    smtp: Optional[SmtpConfig],
    *,
    auth_user: Optional[str] = None,
    auth_password: Optional[str] = None,
):
    install_db_collector(db_path)

    auth_enabled = bool(auth_user and auth_password)
    expected_user = (auth_user or "").encode("utf-8")
    expected_password = (auth_password or "").encode("utf-8")

    sessions: dict[str, float] = {}
    sessions_lock = threading.Lock()

    def _new_session() -> str:
        token = secrets.token_urlsafe(32)
        with sessions_lock:
            sessions[token] = time.time() + _SESSION_TTL_SEC
        return token

    def _drop_session(token: str) -> None:
        with sessions_lock:
            sessions.pop(token, None)

    def _valid_session(token: str) -> bool:
        with sessions_lock:
            exp = sessions.get(token)
            if exp is None:
                return False
            if exp < time.time():
                sessions.pop(token, None)
                return False
            return True

    class Handler(BaseHTTPRequestHandler):

        def log_message(self, fmt: str, *args: object) -> None:
            pass

        def send_response(self, code, message=None):  # type: ignore[override]
            try:
                endpoint = _metrics_endpoint(urlparse(self.path).path)
                WEB_REQUESTS.labels(
                    method=self.command or "GET",
                    endpoint=endpoint,
                    status=str(int(code)),
                ).inc()
            except Exception:
                pass
            super().send_response(code, message)

        def _session_token(self) -> Optional[str]:
            raw = self.headers.get("Cookie", "")
            if not raw:
                return None
            try:
                jar = http.cookies.SimpleCookie()
                jar.load(raw)
            except http.cookies.CookieError:
                return None
            morsel = jar.get(_SESSION_COOKIE)
            return morsel.value if morsel else None

        def _is_authenticated(self) -> bool:
            tok = self._session_token()
            return bool(tok) and _valid_session(tok)

        def _check_auth(self) -> bool:
            """Whitelist + проверка cookie. Для HTML-запросов от браузера —
            редирект на красивую страницу /login?next=…, для API-запросов
            (Accept: application/json) — JSON 401, чтобы фронт мог обработать."""
            if not auth_enabled:
                return True
            path = urlparse(self.path).path
            if path in _UNAUTHENTICATED_PATHS or path.startswith("/static/"):
                return True
            if self._is_authenticated():
                return True

            wants_json = (
                path.startswith("/api/")
                or "application/json" in self.headers.get("Accept", "")
            )
            if wants_json:
                self._json(HTTPStatus.UNAUTHORIZED, {"error": "Требуется вход"})
                return False

            next_url = self.path or "/"
            self.send_response(HTTPStatus.SEE_OTHER)
            self.send_header("Location", f"/login?next={quote(next_url, safe='/')}")
            self.send_header("Content-Length", "0")
            self.end_headers()
            return False

        def _set_session_cookie(self, token: str) -> None:
            parts = [
                f"{_SESSION_COOKIE}={token}",
                "Path=/",
                "HttpOnly",
                "SameSite=Lax",
                f"Max-Age={_SESSION_TTL_SEC}",
            ]
            self.send_header("Set-Cookie", "; ".join(parts))

        def _clear_session_cookie(self) -> None:
            self.send_header(
                "Set-Cookie",
                f"{_SESSION_COOKIE}=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0",
            )

        def _handle_login_get(self) -> None:
            if self._is_authenticated():
                self.send_response(HTTPStatus.SEE_OTHER)
                self.send_header("Location", "/")
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            self._file(_STATIC / "login.html", "text/html; charset=utf-8")

        def _handle_login_post(self) -> None:
            try:
                data = self._read_json_body()
            except ValueError:
                return self._json(HTTPStatus.BAD_REQUEST, {"error": "Неверные данные"})
            user = str(data.get("user", "")).encode("utf-8")
            password = str(data.get("password", "")).encode("utf-8")
            ok_user = hmac.compare_digest(user, expected_user)
            ok_pass = hmac.compare_digest(password, expected_password)
            if not (ok_user and ok_pass):
                return self._json(
                    HTTPStatus.UNAUTHORIZED, {"error": "Неверный логин или пароль"}
                )
            token = _new_session()
            body = json.dumps({"ok": True}).encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self._set_session_cookie(token)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _handle_logout(self) -> None:
            tok = self._session_token()
            if tok:
                _drop_session(tok)
            wants_json = (
                self.command == "POST"
                or "application/json" in self.headers.get("Accept", "")
            )
            if wants_json:
                body = json.dumps({"ok": True}).encode("utf-8")
                self.send_response(HTTPStatus.OK)
                self._clear_session_cookie()
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            self.send_response(HTTPStatus.SEE_OTHER)
            self._clear_session_cookie()
            self.send_header("Location", "/login")
            self.send_header("Content-Length", "0")
            self.end_headers()

        def _json(self, status: int, payload: Any) -> None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _file(self, path: Path, content_type: str) -> None:
            if not path.is_file():
                self.send_error(HTTPStatus.NOT_FOUND)
                return
            data = path.read_bytes()
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            path = parsed.path
            if path == "/login":
                return self._handle_login_get()
            if path == "/logout":
                return self._handle_logout()
            if path == "/static/style.css":
                return self._file(_STATIC / "style.css", "text/css; charset=utf-8")
            if path == "/static/app.js":
                return self._file(_STATIC / "app.js", "application/javascript; charset=utf-8")
            if not self._check_auth():
                return
            if path in ("/", "/index.html"):
                return self._file(_STATIC / "index.html", "text/html; charset=utf-8")
            if path == "/metrics":
                body = METRICS_REGISTRY.render()
                self.send_response(HTTPStatus.OK)
                self.send_header(
                    "Content-Type", "text/plain; version=0.0.4; charset=utf-8"
                )
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            if path == "/api/health":
                payload = json.dumps({"ok": True}).encode("utf-8")
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                return

            qs = parse_qs(parsed.query)
            m_recon = _RECON_SEND.match(path)
            if m_recon:
                return self.send_error(HTTPStatus.METHOD_NOT_ALLOWED)

            m_recon_rows = _RECON_ROWS.match(path)
            m_recon_get = _RECON_GET.match(path)
            m = _EMAIL_PATH.match(path)
            conn = open_database_fast(db_path)
            try:
                if m_recon_rows:
                    rid = int(m_recon_rows.group(1))
                    row = get_reconciliation(conn, rid)
                    if row is None:
                        return self._json(HTTPStatus.NOT_FOUND, {"error": "Сверка не найдена"})
                    rows = confirmation_rows_for_reconciliation(conn, rid)
                    return self._json(
                        HTTPStatus.OK,
                        {
                            "id": rid,
                            "recipient_email": str(row["recipient_email"]),
                            "started_at": row["started_at"],
                            "sent_at": row["sent_at"],
                            "rows": [_row_to_recon_row_api(r) for r in rows],
                        },
                    )
                if path == "/api/settings/intro_text":
                    return self._json(
                        HTTPStatus.OK,
                        {
                            "text": get_digest_intro_text(conn),
                            "default": DEFAULT_DIGEST_INTRO_TEXT,
                        },
                    )
                if path == "/api/companies":


                    search = (qs.get("q") or [""])[0]
                    rows = [_row_to_api(r) for r in list_recipient_triggers(conn, search=search)]
                    return self._json(HTTPStatus.OK, rows)
                if m_recon_get:
                    rid = int(m_recon_get.group(1))
                    row = get_reconciliation(conn, rid)
                    if row is None:
                        return self._json(HTTPStatus.NOT_FOUND, {"error": "Сверка не найдена"})
                    letters = conn.execute(
                        "SELECT COUNT(*) AS c FROM confirmations WHERE reconciliation_id = ?",
                        (rid,),
                    ).fetchone()
                    return self._json(
                        HTTPStatus.OK,
                        {
                            "id": rid,
                            "recipient_email": str(row["recipient_email"]),
                            "started_at": row["started_at"],
                            "sent_at": row["sent_at"],
                            "letter_count": int(letters["c"]) if letters else 0,
                        },
                    )
                if m:
                    email = normalize_recipient_email(unquote(m.group(1)))
                    if m.group(2):
                        items = list_reconciliations_for_recipient(conn, email)
                        return self._json(HTTPStatus.OK, items)
                    row = get_recipient_profile(conn, email)
                    if row is None:
                        return self._json(HTTPStatus.NOT_FOUND, {"error": "Не найдено"})
                    return self._json(HTTPStatus.OK, _row_to_api(row))
            finally:
                conn.close()
            self.send_error(HTTPStatus.NOT_FOUND)

        def _read_json_body(self) -> dict[str, Any]:
            length = int(self.headers.get("Content-Length", 0))
            raw = self.rfile.read(length) if length else b"{}"
            try:
                data = json.loads(raw.decode("utf-8"))
            except json.JSONDecodeError as e:
                raise ValueError("Неверный JSON") from e
            if not isinstance(data, dict):
                raise ValueError("Ожидается JSON-объект")
            return data

        def do_POST(self) -> None:
            path = urlparse(self.path).path
            if path == "/login":
                return self._handle_login_post()
            if path == "/logout":
                return self._handle_logout()
            if not self._check_auth():
                return
            m_send = _RECON_SEND.match(path)
            if m_send:
                rid = int(m_send.group(1))
                conn = open_database_fast(db_path)
                try:
                    if smtp is None:
                        pending = request_immediate_digest_for_reconciliation(conn, rid)
                        return self._json(
                            HTTPStatus.OK,
                            {
                                "ok": True,
                                "queued": True,
                                "message": f"Сводка #{rid} в очереди ({pending} писем)",
                            },
                        )

                    pending = _validate_reconciliation_send(conn, rid)
                finally:
                    conn.close()

                threading.Thread(
                    target=_bg_send_reconciliation,
                    args=(db_path, rid, smtp),
                    daemon=True,
                ).start()
                return self._json(
                    HTTPStatus.OK,
                    {
                        "ok": True,
                        "queued": True,
                        "message": f"Сводка #{rid} ставится в отправку ({pending} писем)",
                    },
                )

            if path == "/api/companies/action":
                try:
                    data = self._read_json_body()
                except ValueError as e:
                    return self._json(HTTPStatus.BAD_REQUEST, {"error": str(e)})
                email = normalize_recipient_email(str(data.get("email", "")))
                action = str(data.get("action", "")).strip()
                if "@" not in email:
                    return self._json(HTTPStatus.BAD_REQUEST, {"error": "Укажите e-mail"})
                conn = open_database_fast(db_path)

                try:
                    if action == "pause":
                        pause_recipient(conn, email)
                        return self._json(HTTPStatus.OK, {"ok": True, "message": "Накопление отключено"})
                    if action == "resume":
                        resume_recipient(conn, email)
                        return self._json(HTTPStatus.OK, {"ok": True, "message": "Накопление включено"})
                    if action == "delete":
                        removed = delete_recipient(conn, email)
                        return self._json(
                            HTTPStatus.OK,
                            {"ok": True, "message": f"Удалено. Сброшено писем: {removed}"},
                        )
                    if action == "send_now":
                        status, payload = _handle_send_now(conn, email, smtp)
                        return self._json(status, payload)
                    return self._json(HTTPStatus.BAD_REQUEST, {"error": "Неизвестное действие"})
                except ValueError as e:
                    return self._json(HTTPStatus.BAD_REQUEST, {"error": str(e)})
                finally:
                    conn.close()

            if path == "/api/settings/intro_text":
                try:
                    data = self._read_json_body()
                except ValueError as e:
                    return self._json(HTTPStatus.BAD_REQUEST, {"error": str(e)})
                text = str(data.get("text", ""))
                if len(text) > 10000:
                    return self._json(
                        HTTPStatus.BAD_REQUEST, {"error": "Слишком длинный текст"}
                    )
                conn = open_database_fast(db_path)
                try:
                    set_digest_intro_text(conn, text)
                finally:
                    conn.close()
                return self._json(HTTPStatus.OK, {"ok": True, "text": text})

            if path == "/api/companies":
                try:
                    data = self._read_json_body()
                    email, trigger_type, cfg, company_name = _validate_payload(data)

                except ValueError as e:
                    return self._json(HTTPStatus.BAD_REQUEST, {"error": str(e)})
                conn = open_database_fast(db_path)

                try:
                    set_recipient_trigger(
                        conn, email, trigger_type, cfg, company_name=company_name
                    )
                finally:
                    conn.close()
                return self._json(HTTPStatus.OK, {"ok": True, "email": email})

            self.send_error(HTTPStatus.NOT_FOUND)

    return Handler

def request_immediate_digest_for_reconciliation(
    conn: sqlite3.Connection, reconciliation_id: int
) -> int:
    from mail_confirm.reconciliations import get_reconciliation

    row = get_reconciliation(conn, reconciliation_id)
    if row is None:
        raise ValueError("Сверка не найдена")
    email = str(row["recipient_email"])
    pending = conn.execute(
        """
        SELECT COUNT(*) AS c FROM confirmations
        WHERE reconciliation_id = ? AND digest_sent_at IS NULL
        """,
        (reconciliation_id,),
    ).fetchone()
    n = int(pending["c"]) if pending else 0
    if n == 0:
        raise ValueError("Нет новых писем для отправки в этой сверке")

    if row["sent_at"] is not None:
        raise ValueError(
            "Старую (закрытую) сверку можно отправить только при настроенном SMTP "
            "у веб-интерфейса. Перезапустите его с SMTP_* переменными окружения."
        )
    request_immediate_digest(conn, email)
    conn.commit()
    return n

def _validate_reconciliation_send(
    conn: sqlite3.Connection, reconciliation_id: int
) -> int:
    """Быстрая проверка перед фоновой SMTP-отправкой: сверка существует,
    в ней есть строки, и она не уже отправлена «вхолостую»."""
    row = get_reconciliation(conn, reconciliation_id)
    if row is None:
        raise ValueError("Сверка не найдена")
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
    total = conn.execute(
        "SELECT COUNT(*) AS c FROM confirmations WHERE reconciliation_id = ?",
        (reconciliation_id,),
    ).fetchone()
    n = int(total["c"]) if total else 0
    if n == 0:
        raise ValueError("В сверке нет писем")
    return n

def _handle_send_now(
    conn: sqlite3.Connection, email: str, smtp: Optional[SmtpConfig]
) -> tuple[int, dict[str, Any]]:
    from mail_confirm.reconciliations import get_open_reconciliation_id, link_orphan_pending_to_open

    link_orphan_pending_to_open(conn, email)
    rid = get_open_reconciliation_id(conn, email)
    if rid is None:
        raise ValueError("Нет открытой сверки для отправки")
    if smtp is not None:

        n = _validate_reconciliation_send(conn, rid)
        threading.Thread(
            target=_bg_send_reconciliation,
            args=(__db_path_for(conn), rid, smtp),
            daemon=True,
        ).start()
        return HTTPStatus.OK, {
            "ok": True,
            "queued": True,
            "message": f"Сводка #{rid} ставится в отправку ({n} писем)",
        }
    n = request_immediate_digest(conn, email)
    return HTTPStatus.OK, {
        "ok": True,
        "queued": True,
        "message": f"Сводка #{rid} поставлена в очередь ({n} писем)",
    }

def __db_path_for(conn: sqlite3.Connection) -> str:
    row = conn.execute("PRAGMA database_list").fetchone()
    return str(row["file"]) if row and row["file"] else ":memory:"

def serve_web_ui(
    *,
    db_path: str,
    host: str,
    port: int,
    smtp: Optional[SmtpConfig] = None,
    auth_user: Optional[str] = None,
    auth_password: Optional[str] = None,
) -> None:

    open_database(db_path).close()
    server = ThreadingHTTPServer(
        (host, port),
        make_handler(db_path, smtp, auth_user=auth_user, auth_password=auth_password),
    )
    mode_parts: list[str] = ["SMTP из окружения" if smtp else "без SMTP (только очередь)"]
    if auth_user and auth_password:
        mode_parts.append(f"вход как {auth_user!r}")
    else:
        mode_parts.append("без аутентификации")
    print(
        f"Веб-интерфейс: http://{host}:{port}/  ({', '.join(mode_parts)})",
        flush=True,
    )
    if not (auth_user and auth_password) and host not in ("127.0.0.1", "localhost", "::1"):
        print(
            f"WEB: внимание — слушаем {host}:{port} без аутентификации. "
            f"Задайте WEB_AUTH_USER / WEB_AUTH_PASSWORD (или флаги --web-auth-user/--web-auth-password), "
            f"иначе любой, кто видит порт, может удалять компании и слать письма.",
            file=sys.stderr,
            flush=True,
        )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nОстановка.", flush=True)
