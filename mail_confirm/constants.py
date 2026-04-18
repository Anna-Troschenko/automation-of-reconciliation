from __future__ import annotations

import re

CONFIRMATION_PATTERN = re.compile(
    r"Добрый\s+день!\s*"
    r"Подтверждаю\s+нежелательное\s+явление\s*(\d+)\s*,\s*"
    r"сопоставленный\s+ID:\s*(\d+)",
    re.IGNORECASE | re.DOTALL,
)

DIGEST_SMTP_SUBJECT = "Сводка подтверждений"
