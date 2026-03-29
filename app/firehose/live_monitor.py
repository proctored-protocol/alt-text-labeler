from __future__ import annotations

import json
import logging
import time
from typing import Any

from atproto import (
    FirehoseSubscribeReposClient,
    models,
    parse_subscribe_repos_message,
)

from app.firehose.live_metrics_store import LiveMetricsStore
from app.parsing.posts import iter_post_creates


logger = logging.getLogger(__name__)


def truncate_error(value: str, limit: int = 2000) -> str:
    value = value.strip()
    if len(value) <= limit:
        return value
    return value[:limit] + "...[truncated]"


def image_alts_from_post(post: Any) -> list[str]:
    value = getattr(post, "image_alts", None)
    if not value:
        return []
    return [str(item) if item is not None else "" for item in value]


def usable_alt_count(alts: list[str]) -> int:
    return sum(1 for alt in alts if alt.strip())


def media_text(post: Any) -> str:
    parts: list[str] = []
    for attr in (
        "raw_embed_type",
        "embed_type",
        "embed_kind",
        "media_type",
        "mime_type",
        "record_type",
    ):
        value = getattr(post, attr, None)
        if value is not None:
            parts.append(str(value).lower())
    return " ".join(parts)


def detect_gif(post: Any) -> bool:
    text = media_text(post)
    return "gif" in text or "giphy" in text or "tenor" in text


def detect_video(post: Any) -> bool:
    text = media_text(post)
    return "video" in text


class FirehoseLiveCollector:
    def __init__(
        self,
        *,
        db_path: str,
        base_uri: str,
        cursor: int | None,
        resume_from_store: bool,
    ) -> None:
        self.base_uri = base_uri
        self.explicit_cursor = cursor
        self.resume_from_store = resume_from_store
        self.store = LiveMetricsStore(db_path)
        self.client: FirehoseSubscribeReposClient | None = None

    def _build_client(self) -> FirehoseSubscribeReposClient:
        cursor = self.explicit_cursor
        if cursor is None and self.resume_from_store:
            cursor = self.store.get_resume_cursor()

        params = {"cursor": cursor} if cursor is not None else None

        logger.info(
            "firehose_live_client_building",
            extra={
                "base_uri": self.base_uri,
                "cursor": cursor,
                "resume_from_store": self.resume_from_store,
            },
        )

        return FirehoseSubscribeReposClient(
            params=params,
            base_uri=self.base_uri,
        )

    def on_callback_error(self, exc: BaseException) -> None:
        msg = truncate_error(str(exc))
        self.store.mark_error(msg)
        logger.exception("firehose_live_callback_error", exc_info=exc)

    def on_message(self, message: Any) -> None:
        parsed = parse_subscribe_repos_message(message)
        if not isinstance(parsed, models.ComAtprotoSyncSubscribeRepos.Commit):
            return
        self._handle_commit(parsed)

    def _handle_commit(self, commit: models.ComAtprotoSyncSubscribeRepos.Commit) -> None:
        counts = {
            "commit_count": 1,
            "post_create_count": 0,
            "image_post_count": 0,
            "missing_alt_post_count": 0,
            "partial_alt_post_count": 0,
            "gif_post_count": 0,
            "video_post_count": 0,
        }

        for post in iter_post_creates(commit):
            counts["post_create_count"] += 1

            alts = image_alts_from_post(post)
            if alts:
                counts["image_post_count"] += 1
                usable = usable_alt_count(alts)

                if usable == 0:
                    counts["missing_alt_post_count"] += 1
                elif len(alts) > 1 and usable < len(alts):
                    counts["partial_alt_post_count"] += 1

            if detect_gif(post):
                counts["gif_post_count"] += 1

            if detect_video(post):
                counts["video_post_count"] += 1

        self.store.record_counts(
            ts_epoch=int(time.time()),
            counts=counts,
            last_seq=getattr(commit, "seq", None),
        )

    def run(self) -> None:
        self.store.set_status("starting")
        reconnect_delay_seconds = 2.0

        while True:
            try:
                self.client = self._build_client()
                self.store.set_status("running")
                self.client.start(self.on_message, self.on_callback_error)

                self.store.mark_reconnect("client stopped unexpectedly, reconnecting")
                time.sleep(reconnect_delay_seconds)

            except Exception as exc:
                msg = truncate_error(str(exc))
                self.store.mark_error(msg)

                if "ConsumerTooSlow" in msg:
                    self.store.mark_reconnect("ConsumerTooSlow")
                    time.sleep(reconnect_delay_seconds)
                    continue

                logger.exception("firehose_live_worker_crashed_reconnecting")
                self.store.mark_reconnect(msg)
                time.sleep(reconnect_delay_seconds)
                continue