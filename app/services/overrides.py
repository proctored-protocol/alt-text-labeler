from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import ManualOverride


def is_uri_suppressed(session: Session, uri: str) -> bool:
    stmt = select(ManualOverride).where(ManualOverride.uri == uri)
    row = session.execute(stmt).scalar_one_or_none()

    if row is None:
        return False

    if row.override_type != "suppress":
        return False

    if row.expires_at is None:
        return True

    return row.expires_at > datetime.now(timezone.utc)