from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from atproto import CAR, FirehoseSubscribeReposClient, models, parse_subscribe_repos_message

from app.config import get_settings
from app.db import SessionLocal, session_scope
from app.head.store import (
    HEAD_TRACKER_CONSUMER_NAME,
    HEAD_TRACKER_CONSUMER_TYPE,
    HeadBucketCounts,
    get_consumer_state,
    record_head_sample,
    upsert_consumer_state,
)
from app.rules.labeling import derive_post_label


logger = logging.getLogger(__name__)

POST_RECORD_TYPE = "app.bsky.feed.post"
POST_PATH_PREFIX = "app.bsky.feed.post/"
IMAGE_EMBED_TYPES = {"app.bsky.embed.images"}
RECORD_WITH_MEDIA_TYPES = {
    "app.bsky.embed.recordWithMedia",
    "app.bsky.embed.record_with_media",
}
VIDEO_EMBED_TYPES = {
    "app.bsky.embed.video",
}
EXTERNAL_EMBED_TYPES = {
    "app.bsky.embed.external",
}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def floor_to_second(value: datetime) -> datetime:
    return value.astimezone(timezone.utc).replace(microsecond=0)


def _as_dict(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return dict(value)
    if hasattr(value, "model_dump"):
        return value.model_dump(exclude_none=True, by_alias=True)
    if hasattr(value, "dict"):
        return value.dict(exclude_none=True, by_alias=True)
    return {}


def _get_type(value: Any) -> str | None:
    data = _as_dict(value)
    return data.get("$type") or data.get("py_type")


def _get_record_type(record: Any) -> str | None:
    record_dict = _as_dict(record)
    return record_dict.get("$type") or record_dict.get("py_type")


def _extract_image_alts_from_embed(embed: Any) -> list[str | None]:
    data = _as_dict(embed)
    embed_type = _get_type(data)

    if embed_type in IMAGE_EMBED_TYPES:
        image_alts: list[str | None] = []
        for image in data.get("images", []):
            image_dict = _as_dict(image)
            image_alts.append(image_dict.get("alt"))
        return image_alts

    if embed_type in RECORD_WITH_MEDIA_TYPES:
        return _extract_image_alts_from_embed(data.get("media"))

    return []


def _extract_media_flags_from_embed(embed: Any) -> tuple[bool, bool]:
    data = _as_dict(embed)
    embed_type = _get_type(data)

    if embed_type in VIDEO_EMBED_TYPES:
        return False, True

    if embed_type in RECORD_WITH_MEDIA_TYPES:
        return _extract_media_flags_from_embed(data.get("media"))

    if embed_type in EXTERNAL_EMBED_TYPES:
        external = _as_dict(data.get("external"))
        uri = str(external.get("uri") or "").lower()
        mime_type = str(external.get("mimeType") or external.get("mime_type") or "").lower()
        is_gif = uri.endswith(".gif") or mime_type == "image/gif"
        return is_gif, False

    return False, False


@dataclass(slots=True)
class HeadBucket:
    bucket_second: datetime
    head_seq: int
    last_observed_at: datetime
    commit_count: int = 0
    post_count: int = 0
    image_post_count: int = 0
    missing_alt_post_count: int = 0
    partial_alt_post_count: int = 0
    gif_post_count: int = 0
    video_post_count: int = 0

    def to_counts(self) -> HeadBucketCounts:
        return HeadBucketCounts(
            commit_count=self.commit_count,
            post_count=self.post_count,
            image_post_count=self.image_post_count,
            missing_alt_post_count=self.missing_alt_post_count,
            partial_alt_post_count=self.partial_alt_post_count,
            gif_post_count=self.gif_post_count,
            video_post_count=self.video_post_count,
        )


class FirehoseHeadTracker:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.client: FirehoseSubscribeReposClient | None = None
        self.started_at = utc_now()
        self.current_bucket: HeadBucket | None = None

        logger.info(
            "head_tracker_initialized",
            extra={
                "base_uri": self.settings.firehose_base_uri,
                "stream_name": self.settings.firehose_stream_name,
            },
        )

    def _choose_start_cursor(self) -> int | None:
        if self.settings.head_tracker_start_cursor is not None:
            logger.info(
                "head_tracker_using_explicit_start_cursor",
                extra={"cursor": self.settings.head_tracker_start_cursor},
            )
            return int(self.settings.head_tracker_start_cursor)

        if not self.settings.head_tracker_resume_from_consumer_state:
            logger.info("head_tracker_starting_without_saved_cursor")
            return None

        with SessionLocal() as session:
            state = get_consumer_state(session, HEAD_TRACKER_CONSUMER_NAME)

        if state is None or state.cursor_seq is None:
            logger.info("head_tracker_no_saved_cursor_found")
            return None

        if state.cursor_observed_at is not None:
            stale_after_seconds = max(self.settings.target_lag_seconds_max * 2, 120)
            cursor_age_seconds = (
                utc_now() - state.cursor_observed_at.astimezone(timezone.utc)
            ).total_seconds()

            if cursor_age_seconds > stale_after_seconds:
                logger.warning(
                    "head_tracker_saved_cursor_ignored_as_stale",
                    extra={
                        "saved_cursor": state.cursor_seq,
                        "cursor_age_seconds": round(cursor_age_seconds, 2),
                        "stale_after_seconds": stale_after_seconds,
                    },
                )
                return None

        logger.info(
            "head_tracker_resuming_from_saved_cursor",
            extra={"cursor": state.cursor_seq},
        )
        return int(state.cursor_seq)

    def _build_client(self) -> FirehoseSubscribeReposClient:
        cursor = self._choose_start_cursor()
        params = {"cursor": cursor} if cursor is not None else None

        logger.info(
            "head_tracker_client_building",
            extra={
                "base_uri": self.settings.firehose_base_uri,
                "cursor": cursor,
            },
        )

        return FirehoseSubscribeReposClient(
            params=params,
            base_uri=self.settings.firehose_base_uri,
        )

    def _ensure_bucket(self, *, observed_at: datetime, seq: int) -> None:
        bucket_second = floor_to_second(observed_at)

        if self.current_bucket is None:
            self.current_bucket = HeadBucket(
                bucket_second=bucket_second,
                head_seq=seq,
                last_observed_at=observed_at,
            )
            return

        if bucket_second != self.current_bucket.bucket_second:
            self._flush_current_bucket(status="running")
            self.current_bucket = HeadBucket(
                bucket_second=bucket_second,
                head_seq=seq,
                last_observed_at=observed_at,
            )
            return

        self.current_bucket.head_seq = max(self.current_bucket.head_seq, seq)
        self.current_bucket.last_observed_at = observed_at

    def _flush_current_bucket(
        self,
        *,
        status: str,
        last_error_code: str | None = None,
        last_error_text: str | None = None,
    ) -> None:
        if self.current_bucket is None:
            return

        bucket = self.current_bucket

        with session_scope() as session:
            record_head_sample(
                session,
                bucket_second=bucket.bucket_second,
                head_seq=bucket.head_seq,
                counts=bucket.to_counts(),
            )
            upsert_consumer_state(
                session,
                consumer_name=HEAD_TRACKER_CONSUMER_NAME,
                consumer_type=HEAD_TRACKER_CONSUMER_TYPE,
                stream_name=self.settings.firehose_stream_name,
                status=status,
                cursor_seq=bucket.head_seq,
                cursor_observed_at=bucket.last_observed_at,
                started_at=self.started_at,
                last_error_code=last_error_code,
                last_error_text=last_error_text,
            )

        logger.info(
            "head_tracker_bucket_flushed",
            extra={
                "bucket_second": bucket.bucket_second.isoformat(),
                "head_seq": bucket.head_seq,
                "commit_count": bucket.commit_count,
                "post_count": bucket.post_count,
                "image_post_count": bucket.image_post_count,
                "missing_alt_post_count": bucket.missing_alt_post_count,
                "partial_alt_post_count": bucket.partial_alt_post_count,
                "gif_post_count": bucket.gif_post_count,
                "video_post_count": bucket.video_post_count,
            },
        )

        self.current_bucket = None

    def _accumulate_commit_metrics(
        self,
        commit: models.ComAtprotoSyncSubscribeRepos.Commit,
        *,
        observed_at: datetime,
    ) -> None:
        self._ensure_bucket(observed_at=observed_at, seq=commit.seq)
        assert self.current_bucket is not None

        self.current_bucket.commit_count += 1
        self.current_bucket.head_seq = max(self.current_bucket.head_seq, commit.seq)
        self.current_bucket.last_observed_at = observed_at

        try:
            car = CAR.from_bytes(commit.blocks)
        except Exception:
            logger.exception(
                "head_tracker_car_decode_failed",
                extra={"seq": commit.seq, "repo": commit.repo},
            )
            return

        for op in commit.ops:
            if op.action != "create" or not op.cid:
                continue

            if not op.path.startswith(POST_PATH_PREFIX):
                continue

            raw_record = car.blocks.get(op.cid)
            record_dict = _as_dict(raw_record)

            if _get_record_type(record_dict) != POST_RECORD_TYPE:
                continue

            self.current_bucket.post_count += 1

            image_alts = _extract_image_alts_from_embed(record_dict.get("embed"))
            has_gif, has_video = _extract_media_flags_from_embed(record_dict.get("embed"))

            if has_gif:
                self.current_bucket.gif_post_count += 1
            if has_video:
                self.current_bucket.video_post_count += 1

            if not image_alts:
                continue

            self.current_bucket.image_post_count += 1

            _image_count, _usable_alt_count, derived_label = derive_post_label(
                image_alts=image_alts,
                missing_label=self.settings.label_missing_alt,
                partial_label=self.settings.label_partial_alt,
            )

            if derived_label == self.settings.label_missing_alt:
                self.current_bucket.missing_alt_post_count += 1
            elif derived_label == self.settings.label_partial_alt:
                self.current_bucket.partial_alt_post_count += 1

    def run(self) -> None:
        reconnect_delay_seconds = 2.0

        with session_scope() as session:
            upsert_consumer_state(
                session,
                consumer_name=HEAD_TRACKER_CONSUMER_NAME,
                consumer_type=HEAD_TRACKER_CONSUMER_TYPE,
                stream_name=self.settings.firehose_stream_name,
                status="starting",
                cursor_seq=None,
                cursor_observed_at=None,
                started_at=self.started_at,
                last_error_code=None,
                last_error_text=None,
            )

        while True:
            try:
                self.client = self._build_client()
                self.client.start(self.on_message, self.on_callback_error)

                self._flush_current_bucket(status="running")

                with session_scope() as session:
                    upsert_consumer_state(
                        session,
                        consumer_name=HEAD_TRACKER_CONSUMER_NAME,
                        consumer_type=HEAD_TRACKER_CONSUMER_TYPE,
                        stream_name=self.settings.firehose_stream_name,
                        status="starting",
                        cursor_seq=None,
                        cursor_observed_at=None,
                        started_at=self.started_at,
                        last_error_code="client_stopped",
                        last_error_text="Firehose client stopped unexpectedly and will reconnect.",
                    )

                logger.warning(
                    "head_tracker_client_stopped_reconnecting",
                    extra={"reconnect_delay_seconds": reconnect_delay_seconds},
                )
                time.sleep(reconnect_delay_seconds)

            except Exception as exc:
                error_text = str(exc).strip() or exc.__class__.__name__
                self._flush_current_bucket(
                    status="error",
                    last_error_code=exc.__class__.__name__,
                    last_error_text=error_text,
                )

                with session_scope() as session:
                    upsert_consumer_state(
                        session,
                        consumer_name=HEAD_TRACKER_CONSUMER_NAME,
                        consumer_type=HEAD_TRACKER_CONSUMER_TYPE,
                        stream_name=self.settings.firehose_stream_name,
                        status="error",
                        cursor_seq=None,
                        cursor_observed_at=None,
                        started_at=self.started_at,
                        last_error_code=exc.__class__.__name__,
                        last_error_text=error_text,
                    )

                logger.exception("head_tracker_crashed_reconnecting")
                time.sleep(reconnect_delay_seconds)

    def on_callback_error(self, exc: BaseException) -> None:
        logger.exception("head_tracker_callback_error", exc_info=exc)

    def on_message(self, message) -> None:
        parsed = parse_subscribe_repos_message(message)
        if not isinstance(parsed, models.ComAtprotoSyncSubscribeRepos.Commit):
            return

        observed_at = utc_now()
        self._accumulate_commit_metrics(parsed, observed_at=observed_at)