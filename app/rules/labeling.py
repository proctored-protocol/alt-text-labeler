from __future__ import annotations

from dataclasses import dataclass

from app.rules.alt_text import is_usable_alt_text


DECISION_NO_LABEL = "no_label"
DECISION_MISSING_ALT = "missing-alt-text"
DECISION_PARTIAL_ALT = "partial-alt-text"


@dataclass(frozen=True, slots=True)
class LabelDecisionResult:
    image_count: int
    usable_alt_count: int
    decision_outcome: str
    decision_reason: str | None
    publish_required: bool
    label_value: str | None


def derive_post_label(
    *,
    image_alts: list[str | None],
    missing_label: str,
    partial_label: str,
) -> LabelDecisionResult:
    """
    Derive the canonical label decision for one post.

    Rules:
    - zero images -> no label
    - zero usable alts across one or more images -> missing-alt-text
    - some but not all usable alts -> partial-alt-text
    - all images have usable alt text -> no label
    """
    image_count = len(image_alts)
    usable_alt_count = sum(1 for alt in image_alts if is_usable_alt_text(alt))

    if image_count == 0:
        return LabelDecisionResult(
            image_count=image_count,
            usable_alt_count=usable_alt_count,
            decision_outcome=DECISION_NO_LABEL,
            decision_reason="no_images",
            publish_required=False,
            label_value=None,
        )

    if usable_alt_count == 0:
        return LabelDecisionResult(
            image_count=image_count,
            usable_alt_count=usable_alt_count,
            decision_outcome=missing_label,
            decision_reason="all_image_alt_missing",
            publish_required=True,
            label_value=missing_label,
        )

    if usable_alt_count < image_count:
        return LabelDecisionResult(
            image_count=image_count,
            usable_alt_count=usable_alt_count,
            decision_outcome=partial_label,
            decision_reason="some_image_alt_missing",
            publish_required=True,
            label_value=partial_label,
        )

    return LabelDecisionResult(
        image_count=image_count,
        usable_alt_count=usable_alt_count,
        decision_outcome=DECISION_NO_LABEL,
        decision_reason="all_image_alt_present",
        publish_required=False,
        label_value=None,
    )