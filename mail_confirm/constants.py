from __future__ import annotations

import re

CONFIRMATION_PATTERN = re.compile(
    r"Подтверждаю\s+нежелательное\s+явление\s+([A-Za-zА-Яа-я0-9_\-]+)\s*,\s*"
    r"сопоставленный\s+ID\s*:\s*([A-Za-zА-Яа-я0-9_\-]+)"
    r"(?:\s*\.?\s*Дата\s+явления\s*:\s*(\d{1,2}[.\-/]\d{1,2}[.\-/]\d{2,4}))?",
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
