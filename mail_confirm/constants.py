from __future__ import annotations

import re

_DATE_FIELDS = (
    r"(?:\s*\.?\s*Дата\s+явления\s*:?\s*(\d{1,2}[.\-/]\d{1,2}[.\-/]\d{2,4}))?"
    r"(?:\s*\.?\s*Дата\s+получения\s*:?\s*(\d{1,2}[.\-/]\d{1,2}[.\-/]\d{2,4}))?"
)

CONFIRMATION_PATTERN = re.compile(
    r"Подтверждаю\s+нежелательное\s+явление\s+([A-Za-zА-Яа-я0-9_\-]+)\s*,\s*"
    r"сопоставленный\s+ID\s*:\s*([A-Za-zА-Яа-я0-9_\-]+)"
    + _DATE_FIELDS,
    re.IGNORECASE | re.DOTALL,
)

DELETION_PATTERN = re.compile(
    r"Удаление\s+нежелательного\s+явления\s+([A-Za-zА-Яа-я0-9_\-]+)\s*,\s*"
    r"сопоставленный\s+ID\s*:\s*([A-Za-zА-Яа-я0-9_\-]+)"
    + _DATE_FIELDS,
    re.IGNORECASE | re.DOTALL,
)

APPEND_RECONCILIATION_PATTERN = re.compile(
    r"Дополнение\s+в\s+сверку\s*(?:№|#)?\s*(\d+)",
    re.IGNORECASE,
)

END_OF_RECONCILIATION_PATTERN = re.compile(
    r"Окончание\s+редактирования\s+сверки\s*\.?",
    re.IGNORECASE,
)

DIGEST_SMTP_SUBJECT = "Сводка подтверждений"
