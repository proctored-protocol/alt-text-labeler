from __future__ import annotations

import re


WHITESPACE_RE = re.compile(r"\s+")


def normalize_alt_text(value: str | None) -> str:
    """
    Normalize alt text for evaluation.

    Rules:
    - None becomes empty string
    - leading/trailing whitespace is stripped
    - internal whitespace is collapsed to single spaces
    """
    if value is None:
        return ""

    text = str(value).strip()
    if not text:
        return ""

    return WHITESPACE_RE.sub(" ", text)


def is_usable_alt_text(value: str | None) -> bool:
    """
    Return True if the alt text counts as usable.

    Current v2 rule:
    - blank / whitespace-only alt text is not usable
    - any non-empty normalized text is usable

    This is intentionally conservative and easy to reason about.
    We can tighten this later if we decide to exclude obvious placeholders.
    """
    return normalize_alt_text(value) != ""