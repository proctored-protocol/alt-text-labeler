from datetime import datetime, timezone
from typing import Any

from sqlalchemy.orm import Session

from app.config import get_settings
from app.integrations.ozone.client import ozone_post
from app.models import LabelPublication


def _created_by_did() -> str:
    settings = get_settings()

    if not settings.ozone_proxy_did:
        raise RuntimeError("OZONE_PROXY_DID must be set in .env")

    # OZONE_PROXY_DID looks like:
    # did:plc:rh3vjqs4npfpmnkkmx4u4bzj#atproto_labeler
    return settings.ozone_proxy_did.split("#", 1)[0]


def _build_emit_event_payload(
    *,
    subject_uri: str,
    subject_cid: str,
    label_value: str,
) -> dict[str, Any]:
    return {
        "event": {
            "$type": "tools.ozone.moderation.defs#modEventLabel",
            "createLabelVals": [label_value],
            "negateLabelVals": [],
            "comment": f"Auto-applied by alt-labeler at {datetime.now(timezone.utc).isoformat()}",
        },
        "subject": {
            "$type": "com.atproto.repo.strongRef",
            "uri": subject_uri,
            "cid": subject_cid,
        },
        "createdBy": _created_by_did(),
    }


def enqueue_label_publication(
    session: Session,
    uri: str,
    cid: str,
    label_value: str,
) -> LabelPublication:
    existing = (
        session.query(LabelPublication)
        .filter(
            LabelPublication.uri == uri,
            LabelPublication.cid == cid,
            LabelPublication.label_value == label_value,
        )
        .one_or_none()
    )
    if existing is not None:
        return existing

    row = LabelPublication(
        uri=uri,
        cid=cid,
        label_value=label_value,
        status="pending",
    )
    session.add(row)
    session.flush()
    return row


def publish_label_via_ozone(
    session: Session,
    *,
    uri: str,
    cid: str,
    label_value: str,
) -> LabelPublication:
    row = enqueue_label_publication(session=session, uri=uri, cid=cid, label_value=label_value)

    if row.status == "published":
        return row

    payload = _build_emit_event_payload(
        subject_uri=uri,
        subject_cid=cid,
        label_value=label_value,
    )

    try:
        ozone_post("tools.ozone.moderation.emitEvent", payload)
        row.status = "published"
        row.published_at = datetime.now(timezone.utc)
        row.error_text = None
    except Exception as exc:
        row.status = "failed"
        row.error_text = str(exc)
        raise

    return row