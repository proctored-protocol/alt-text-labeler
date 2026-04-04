from __future__ import annotations

from collections.abc import Iterator, Mapping
from datetime import datetime
from typing import Any

from atproto import CAR, models

from app.parsing.embeds import extract_image_alts_from_record
from app.schemas import ParsedPostCreate


POST_RECORD_TYPE = "app.bsky.feed.post"
POST_PATH_PREFIX = "app.bsky.feed.post/"


def _as_dict(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, Mapping):
        return dict(value)
    if hasattr(value, "model_dump"):
        return value.model_dump(exclude_none=True, by_alias=True)
    if hasattr(value, "dict"):
        return value.dict(exclude_none=True, by_alias=True)
    return {}


def _get_record_type(record: Any) -> str | None:
    record_dict = _as_dict(record)
    return record_dict.get("$type") or record_dict.get("py_type")


def _parse_record_created_at(value: Any) -> datetime | None:
    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None


def iter_post_creates(
    commit: models.ComAtprotoSyncSubscribeRepos.Commit,
) -> Iterator[ParsedPostCreate]:
    car = CAR.from_bytes(commit.blocks)

    for op in commit.ops:
        if op.action != "create" or not op.cid:
            continue

        if not op.path.startswith(POST_PATH_PREFIX):
            continue

        raw_record = car.blocks.get(op.cid)
        record_dict = _as_dict(raw_record)

        if _get_record_type(record_dict) != POST_RECORD_TYPE:
            continue

        embed = record_dict.get("embed") or {}
        raw_embed_type = None
        if isinstance(embed, Mapping):
            raw_embed_type = embed.get("$type") or embed.get("py_type")

        yield ParsedPostCreate(
            uri=f"at://{commit.repo}/{op.path}",
            cid=str(op.cid),
            repo_did=commit.repo,
            author_did=commit.repo,
            path=op.path,
            record_created_at=_parse_record_created_at(
                record_dict.get("createdAt") or record_dict.get("created_at")
            ),
            image_alts=extract_image_alts_from_record(record_dict),
            raw_embed_type=raw_embed_type,
        )