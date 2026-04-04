from __future__ import annotations

from collections.abc import Mapping
from typing import Any


IMAGE_EMBED_TYPES = {"app.bsky.embed.images"}
RECORD_WITH_MEDIA_TYPES = {
    "app.bsky.embed.recordWithMedia",
    "app.bsky.embed.record_with_media",
}


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


def _get_type(value: Any) -> str | None:
    data = _as_dict(value)
    return data.get("$type") or data.get("py_type")


def extract_image_alts_from_embed(embed: Any) -> list[str | None]:
    data = _as_dict(embed)
    embed_type = _get_type(data)

    if embed_type in IMAGE_EMBED_TYPES:
        image_alts: list[str | None] = []
        for image in data.get("images", []):
            image_dict = _as_dict(image)
            image_alts.append(image_dict.get("alt"))
        return image_alts

    if embed_type in RECORD_WITH_MEDIA_TYPES:
        media = data.get("media")
        return extract_image_alts_from_embed(media)

    return []


def extract_image_alts_from_record(record: Mapping[str, Any]) -> list[str | None]:
    return extract_image_alts_from_embed(record.get("embed"))