def normalize_alt_text(value: str | None) -> str | None:
    if value is None:
        return None

    normalized = value.strip()
    return normalized or None


def is_usable_alt_text(value: str | None) -> bool:
    return normalize_alt_text(value) is not None