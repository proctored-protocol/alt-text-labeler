from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class ParsedPostCreate:
    uri: str
    cid: str
    repo_did: str
    author_did: str
    path: str
    record_created_at: datetime | None
    image_alts: list[str | None]
    raw_embed_type: str | None

    @property
    def image_count(self) -> int:
        return len(self.image_alts)