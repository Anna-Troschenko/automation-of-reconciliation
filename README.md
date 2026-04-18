# Формирование сводок

Подключение к почте по IMAP, чтение отправленных писем, нахождение писем заданного формата, сохранение в базу данных, отправление сводки по триггеру (для каждого пользователя можно задать свой триггер).

## Требования

- **Python 3.14+**

## Быстрый старт

Из корня проекта:

```bash
export IMAP_HOST="imap.gmail.com"
export IMAP_USER="you@gmail.com"
export IMAP_PASSWORD="пароль-приложения"
export IMAP_SENT_FOLDER="[Gmail]/Sent Mail"
```

Проверка парсинга писем без записи в БД:

```bash
python3 gmail_parse_sent.py --dry-run
```

Фоновый режим (парсинг писем + отправка сводок по триггерам):

```bash
python3 gmail_parse_sent.py --daemon
```

Справка:

```bash
python3 gmail_parse_sent.py --help
```

Настройка триггеров:

```bash
python3 script.py --set-recipient-interval user@example.com time
```