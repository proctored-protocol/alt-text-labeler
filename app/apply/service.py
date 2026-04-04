from __future__ import annotations

from dataclasses import dataclass

from app.models import IntakeItem, ManualOverride
from app.rules.labeling import (
    DECISION_MISSING_ALT,
    DECISION_NO_LABEL,
    DECISION_PARTIAL_ALT,
    derive_post_label,
)


SUPPRESS_OVERRIDE_TYPES = {
    "suppress",
}

FORCE_MISSING_OVERRIDE_TYPES = {
    DECISION_MISSING_ALT,
    "force-missing-alt-text",
    "force_missing_alt_text",
}

FORCE_PARTIAL_OVERRIDE_TYPES = {
    DECISION_PARTIAL_ALT,
    "force-partial-alt-text",
    "force_partial_alt_text",
}


@dataclass(frozen=True, slots=True)
class ApplyDecision:
    image_count: int
    usable_alt_count: int
    decision_outcome: str
    decision_reason: str | None
    publish_required: bool
    label_value: str | None
    override_applied: bool


def _normalize_override_type(value: str | None) -> str:
    if value is None:
        return ""
    return value.strip().lower()


def evaluate_intake_item(
    *,
    intake_item: IntakeItem,
    rule_version: str,
    missing_label: str,
    partial_label: str,
    manual_override: ManualOverride | None,
) -> ApplyDecision:
    base = derive_post_label(
        image_alts=list(intake_item.image_alts_json or []),
        missing_label=missing_label,
        partial_label=partial_label,
    )

    override_type = _normalize_override_type(
        manual_override.override_type if manual_override is not None else None
    )

    if override_type in SUPPRESS_OVERRIDE_TYPES:
        return ApplyDecision(
            image_count=base.image_count,
            usable_alt_count=base.usable_alt_count,
            decision_outcome=DECISION_NO_LABEL,
            decision_reason="manual_override_suppress",
            publish_required=False,
            label_value=None,
            override_applied=True,
        )

    if override_type in FORCE_MISSING_OVERRIDE_TYPES:
        return ApplyDecision(
            image_count=base.image_count,
            usable_alt_count=base.usable_alt_count,
            decision_outcome=missing_label,
            decision_reason="manual_override_force_missing",
            publish_required=True,
            label_value=missing_label,
            override_applied=True,
        )

    if override_type in FORCE_PARTIAL_OVERRIDE_TYPES:
        return ApplyDecision(
            image_count=base.image_count,
            usable_alt_count=base.usable_alt_count,
            decision_outcome=partial_label,
            decision_reason="manual_override_force_partial",
            publish_required=True,
            label_value=partial_label,
            override_applied=True,
        )

    return ApplyDecision(
        image_count=base.image_count,
        usable_alt_count=base.usable_alt_count,
        decision_outcome=base.decision_outcome,
        decision_reason=base.decision_reason,
        publish_required=base.publish_required,
        label_value=base.label_value,
        override_applied=False,
    )