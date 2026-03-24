from __future__ import annotations

from app.rules.alt_text import is_usable_alt_text


def derive_post_label(
    image_alts: list[str | None],
    missing_label: str,
    partial_label: str,
) -> tuple[int, int, str | None]:
    image_count = len(image_alts)
    usable_alt_count = sum(1 for alt in image_alts if is_usable_alt_text(alt))

    if image_count == 0:
        return image_count, usable_alt_count, None

    if usable_alt_count == 0:
        return image_count, usable_alt_count, missing_label

    if usable_alt_count < image_count:
        return image_count, usable_alt_count, partial_label

    return image_count, usable_alt_count, None