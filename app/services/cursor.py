from sqlalchemy.orm import Session

from app.models import FirehoseCursor


STREAM_NAME = "subscribe_repos"


def get_saved_cursor(session: Session, stream_name: str = STREAM_NAME) -> int | None:
    row = session.get(FirehoseCursor, stream_name)
    return row.last_seq if row else None


def save_cursor(session: Session, seq: int, stream_name: str = STREAM_NAME) -> None:
    row = session.get(FirehoseCursor, stream_name)

    if row is None:
        row = FirehoseCursor(stream_name=stream_name, last_seq=seq)
        session.add(row)
        return

    row.last_seq = seq