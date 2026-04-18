from __future__ import annotations

import argparse
import imaplib
import sqlite3
import sys
import time
from typing import Optional

from mail_confirm.db import (
    daemon_imap_idle_chunk_sec,
    daemon_poll_sec,
    open_database,
    set_recipient_interval,
)
from mail_confirm.imap_client import (
    imap_connect,
    idle_wait_sent_folder,
    imap_supports_idle,
    list_folders,
    select_folder,
)
from mail_confirm.scanner import scan_sent_and_store
from mail_confirm.smtp_ops import (
    default_smtp_host,
    send_due_digests,
    warn_digest_interval_waiting,
)
from mail_confirm.utils import env_first


def needs_imap(args: argparse.Namespace) -> bool:
    if args.daemon or args.list_folders:
        return True
    if args.set_recipient_interval or args.list_recipient_settings:
        return False
    return True


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Парсинг подтверждений из отправленных писем по IMAP."
    )
    parser.add_argument(
        "--host",
        default=env_first("IMAP_HOST"),
        help="IMAP-сервер (или IMAP_HOST)",
    )
    parser.add_argument(
        "--user",
        default=env_first("IMAP_USER", "MAIL_USER", "GMAIL_USER"),
        help="Логин (IMAP_USER / MAIL_USER / GMAIL_USER)",
    )
    parser.add_argument(
        "--password",
        default=env_first("IMAP_PASSWORD", "MAIL_PASSWORD", "GMAIL_PASSWORD"),
        help="Пароль (лучше задать через IMAP_PASSWORD в окружении)",
    )
    parser.add_argument(
        "--sent-folder",
        default=env_first(
            "IMAP_SENT_FOLDER",
            "IMAP_SENT",
            "GMAIL_SENT",
            default="[Gmail]/Sent Mail",
        ),
        help='Папка «Отправленные» (Gmail RU часто: "[Gmail]/Отправленные")',
    )
    parser.add_argument(
        "--port",
        type=int,
        default=None,
        help="Порт (по умолчанию: 993 при SSL, 143 без SSL)",
    )
    parser.add_argument(
        "--ssl",
        dest="use_ssl",
        action="store_true",
        default=None,
        help="TLS с самого начала (IMAP4_SSL), по умолчанию включено",
    )
    parser.add_argument(
        "--no-ssl",
        dest="use_ssl",
        action="store_false",
        help="Без SSL на соединении (часто вместе с --starttls)",
    )
    parser.add_argument(
        "--starttls",
        action="store_true",
        help="Обычный порт, затем STARTTLS (типично порт 143)",
    )
    parser.add_argument(
        "--db",
        default=env_first("MAIL_DB", "GMAIL_DB", default="confirmations.sqlite"),
        help="Путь к файлу SQLite",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Обработать только последние N писем в папке (в демоне — максимум за один опрос)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Только вывести найденные пары (человекочитаемо), без записи в БД",
    )
    parser.add_argument(
        "--stdout-only",
        action="store_true",
        help="Не писать в БД; в stdout — только строки нужного формата (по одной на письмо)",
    )
    parser.add_argument(
        "--list-folders",
        action="store_true",
        help="Показать IMAP LIST папок и выйти",
    )
    parser.add_argument(
        "--digest-interval-sec",
        type=int,
        default=int(env_first("DIGEST_INTERVAL_SEC", default="300") or "300"),
        help="Интервал по умолчанию (сек) для новых получателей и для тех, кого нет в таблице",
    )
    parser.add_argument(
        "--set-recipient-interval",
        nargs=2,
        metavar=("EMAIL", "SECONDS"),
        action="append",
        default=[],
        help="Задать интервал сводки для адреса (можно повторять). Без IMAP, только БД.",
    )
    parser.add_argument(
        "--list-recipient-settings",
        action="store_true",
        help="Показать таблицу recipient_digest и выйти (без IMAP)",
    )
    parser.add_argument(
        "--daemon",
        action="store_true",
        help="Крутиться в фоне: опрос IMAP и отправка сводок по SMTP",
    )
    parser.add_argument(
        "--imap-poll-sec",
        type=int,
        default=int(env_first("IMAP_POLL_SEC", default="60") or "60"),
        help="Пауза между опросами IMAP в --daemon, если IDLE недоступен или отключён",
    )
    parser.add_argument(
        "--imap-idle",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="В --daemon использовать IMAP IDLE: реакция на новое письмо в папке (Python 3.14+)",
    )
    parser.add_argument(
        "--imap-idle-chunk-sec",
        type=float,
        default=float(env_first("IMAP_IDLE_CHUNK_SEC", default="1500") or "1500"),
        help="Сколько секунд держать одну сессию IDLE до продления (избегать обрыва ~30 мин сервером)",
    )
    parser.add_argument(
        "--use-uid-cursor",
        action="store_true",
        help="В БД хранить последний UID отправленных; обрабатывать только новые письма "
        "(для --daemon рекомендуется)",
    )
    parser.add_argument(
        "--smtp-host",
        default=env_first("SMTP_HOST"),
        help="SMTP для сводок (по умолчанию: для Gmail — smtp.gmail.com, иначе как --host)",
    )
    parser.add_argument(
        "--smtp-port",
        type=int,
        default=int(env_first("SMTP_PORT", default="587") or "587"),
        help="SMTP порт (обычно 587 + STARTTLS)",
    )
    parser.add_argument(
        "--smtp-user",
        default=env_first("SMTP_USER"),
        help="Логин SMTP (по умолчанию как --user)",
    )
    parser.add_argument(
        "--smtp-password",
        default=env_first("SMTP_PASSWORD"),
        help="Пароль SMTP (по умолчанию как --password)",
    )
    parser.add_argument(
        "--mail-from",
        default=env_first("MAIL_FROM"),
        help="Заголовок From при отправке сводок (по умолчанию как IMAP user)",
    )
    parser.add_argument(
        "--no-smtp-digest",
        action="store_true",
        help="Не отправлять сводки по SMTP (только запись в БД)",
    )
    args = parser.parse_args()

    if args.use_ssl is None:
        args.use_ssl = not args.starttls

    port = args.port
    if port is None:
        port = 993 if args.use_ssl else 143

    conn: Optional[sqlite3.Connection] = None
    mail: Optional[imaplib.IMAP4] = None
    try:
        if args.set_recipient_interval or args.list_recipient_settings:
            conn = open_database(args.db)
            for pair in args.set_recipient_interval:
                em, sec_s = pair[0], pair[1]
                set_recipient_interval(conn, em, int(sec_s))
                print(f"OK: {em.lower()} → {sec_s} сек", file=sys.stderr)
            if args.list_recipient_settings:
                for row in conn.execute(
                    "SELECT email, interval_seconds, last_digest_sent_at FROM recipient_digest "
                    "ORDER BY email COLLATE NOCASE"
                ):
                    print(
                        f"{row['email']}\tinterval={row['interval_seconds']}s\t"
                        f"last_digest={row['last_digest_sent_at']}"
                    )
            if not needs_imap(args):
                return 0

        if needs_imap(args):
            missing: list[str] = []
            if not args.host:
                missing.append("host (IMAP_HOST)")
            if not args.user:
                missing.append("user (IMAP_USER / MAIL_USER / GMAIL_USER)")
            if not args.password:
                missing.append("password (IMAP_PASSWORD / …)")
            if missing:
                print("Не заданы: " + ", ".join(missing) + ".", file=sys.stderr)
                print(
                    "Нужны все три: хост, логин и пароль (или только --db-команды "
                    "без IMAP: --set-recipient-interval, --list-recipient-settings).",
                    file=sys.stderr,
                )
                return 1

        smtp_host = args.smtp_host or default_smtp_host(args.host)
        smtp_user = args.smtp_user or args.user
        smtp_password = args.smtp_password or args.password
        mail_from = args.mail_from or args.user

        use_db = not args.dry_run and not args.stdout_only
        if use_db and not args.list_folders and conn is None:
            conn = open_database(args.db)

        if needs_imap(args):
            if args.list_folders:
                mail = imap_connect(
                    args.host,
                    port,
                    args.user,
                    args.password,
                    use_ssl=bool(args.use_ssl),
                    use_starttls=args.starttls,
                )
                list_folders(mail)
                return 0

            if args.daemon:
                if conn is None:
                    conn = open_database(args.db)
                assert conn is not None
                poll_sec = max(5, args.imap_poll_sec)
                use_uid = True

                while True:
                    try:
                        mail = imap_connect(
                            args.host,
                            port,
                            args.user,
                            args.password,
                            use_ssl=bool(args.use_ssl),
                            use_starttls=args.starttls,
                        )
                        select_folder(mail, args.sent_folder)
                        idle_ok = bool(args.imap_idle) and imap_supports_idle(mail)
                        if idle_ok:
                            _ic = daemon_imap_idle_chunk_sec(
                                conn,
                                imap_cap=args.imap_idle_chunk_sec,
                                digest_default=args.digest_interval_sec,
                            )
                            print(
                                f"Демон: IMAP IDLE (до {_ic:.0f}s за цикл при неотпр. сводках) + UID-курсор; "
                                f"SMTP {smtp_host}:{args.smtp_port}; деф. интервал {args.digest_interval_sec}s",
                                file=sys.stderr,
                            )
                        else:
                            print(
                                f"Демон: опрос каждые {poll_sec}s (IDLE "
                                f"{'выключён' if not args.imap_idle else 'недоступен'}"
                                f"), UID-курсор; SMTP {smtp_host}:{args.smtp_port}",
                                file=sys.stderr,
                            )

                        def scan_and_digest() -> None:
                            ins, skip, _ = scan_sent_and_store(
                                mail,
                                conn,
                                sent_folder=args.sent_folder,
                                limit=args.limit,
                                use_uid_cursor=use_uid,
                                stdout_only=False,
                                dry_run=False,
                                default_digest_interval=args.digest_interval_sec,
                            )
                            if ins or skip:
                                print(
                                    f"IMAP: добавлено {ins}, пропущено {skip}",
                                    file=sys.stderr,
                                )
                            if not args.no_smtp_digest:
                                try:
                                    n = send_due_digests(
                                        conn,
                                        default_interval=args.digest_interval_sec,
                                        smtp_host=smtp_host,
                                        smtp_port=args.smtp_port,
                                        smtp_user=smtp_user or "",
                                        smtp_password=smtp_password or "",
                                        mail_from=mail_from or "",
                                        dry_run=False,
                                    )
                                    if n > 0:
                                        print(
                                            f"SMTP: отправлено сводок: {n}",
                                            file=sys.stderr,
                                        )
                                except Exception as e:
                                    print(f"SMTP сводки: {e}", file=sys.stderr)

                        scan_and_digest()
                        while True:
                            if idle_ok:
                                idle_chunk = daemon_imap_idle_chunk_sec(
                                    conn,
                                    imap_cap=args.imap_idle_chunk_sec,
                                    digest_default=args.digest_interval_sec,
                                )
                                wake = idle_wait_sent_folder(mail, idle_chunk)
                                if wake == "unsupported":
                                    idle_ok = False
                                    print("IDLE перестал работать, переключаюсь на опрос.", file=sys.stderr)
                                    time.sleep(
                                        daemon_poll_sec(
                                            conn, poll_cap=poll_sec, digest_default=args.digest_interval_sec
                                        )
                                    )
                                elif wake == "exists":
                                    print("IMAP: EXISTS — новое письмо в папке, синхронизация.", file=sys.stderr)
                                scan_and_digest()
                            else:
                                time.sleep(
                                    daemon_poll_sec(
                                        conn, poll_cap=poll_sec, digest_default=args.digest_interval_sec
                                    )
                                )
                                scan_and_digest()
                    except (imaplib.IMAP4.error, OSError) as e:
                        print(f"IMAP: соединение: {e}, переподключение через {poll_sec}s", file=sys.stderr)
                        time.sleep(poll_sec)
                    except Exception as e:
                        print(f"Демон: {e}", file=sys.stderr)
                        time.sleep(poll_sec)
                    finally:
                        if mail is not None:
                            try:
                                mail.logout()
                            except Exception:
                                pass
                            mail = None

            mail = imap_connect(
                args.host,
                port,
                args.user,
                args.password,
                use_ssl=bool(args.use_ssl),
                use_starttls=args.starttls,
            )
            select_folder(mail, args.sent_folder)
            use_uid = args.use_uid_cursor
            inserted, skipped, _ = scan_sent_and_store(
                mail,
                conn if use_db else None,
                sent_folder=args.sent_folder,
                limit=args.limit,
                use_uid_cursor=use_uid,
                stdout_only=args.stdout_only,
                dry_run=args.dry_run,
                default_digest_interval=args.digest_interval_sec,
            )
            if args.stdout_only or args.dry_run:
                pass
            elif conn is not None:
                print(f"Добавлено записей: {inserted}, пропущено (уже в БД или без получателя): {skipped}")
                if not args.no_smtp_digest:
                    try:
                        n = send_due_digests(
                            conn,
                            default_interval=args.digest_interval_sec,
                            smtp_host=smtp_host,
                            smtp_port=args.smtp_port,
                            smtp_user=smtp_user or "",
                            smtp_password=smtp_password or "",
                            mail_from=mail_from or "",
                            dry_run=False,
                        )
                        warn_digest_interval_waiting(
                            conn,
                            args.digest_interval_sec,
                            after_new_inserts=inserted,
                            sent=n,
                        )
                        if n > 1:
                            print(f"Отправлено сводок по SMTP: {n}", file=sys.stderr)
                    except Exception as e:
                        print(f"SMTP сводки: {e}", file=sys.stderr)

    except imaplib.IMAP4.error as e:
        print(f"IMAP: {e}", file=sys.stderr)
        return 1
    except OSError as e:
        print(f"Сеть/сервер: {e}", file=sys.stderr)
        return 1
    except RuntimeError as e:
        print(e, file=sys.stderr)
        return 1
    finally:
        if mail is not None:
            try:
                mail.logout()
            except Exception:
                pass
        if conn is not None:
            conn.close()

    return 0
