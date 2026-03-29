from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import ManualOverride


def is_uri_suppressed(session: Session, uri: str) -> bool:
    return uri in get_suppressed_uris(session, [uri])


def get_suppressed_uris(session: Session, uris: list[str]) -> set[str]:
    if not uris:
        return set()

    now = datetime.now(timezone.utc)

    stmt = (
        select(ManualOverride.uri)
        .where(ManualOverride.uri.in_(uris))
        .where(ManualOverride.override_type == "suppress")
        .where(
            (ManualOverride.expires_at.is_(None))
            | (ManualOverride.expires_at > now)
        )
    )

    rows = session.execute(stmt).all()
    return {row[0] for row in rows}