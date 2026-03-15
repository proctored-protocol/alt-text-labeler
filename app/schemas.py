from dataclasses import dataclass
from typing import Any


@dataclass(slots=True)
class ParsedPostCreate:
    uri: str
    cid: str
    repo_did: str
    author_did: str
    path: str
    created_at: str | None
    image_alts: list[str | None]
    raw_record: dict[str, Any]
    raw_embed_type: str | None


@dataclass(slots=True)
class EvaluationResult:
    uri: str
    cid: str
    author_did: str
    repo_did: str
    image_count: int
    usable_alt_count: int
    derived_label: str | None
    record_created_at: str | None
    raw_embed_type: str | None
    last_seen_seq: int | None