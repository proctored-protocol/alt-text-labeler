import json
import logging
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import text

from app.config import get_settings
from app.db import engine
from app.logging import configure_logging

logger = logging.getLogger(__name__)


class VerifierSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    verifier_labeler_did: str = Field(...)
    verifier_appview_url: str = Field(default="https://bsky.social")
    verifier_batch_size: int = Field(default=100)
    verifier_sleep_seconds: float = Field(default=15.0)
    verifier_lookback_hours: int = Field(default=48)
    verifier_request_timeout_seconds: int = Field(default=20)


@dataclass
class SessionState:
    access_jwt: str


class HTTPJSONError(Exception):
    def __init__(self, code: int, body_text: str):
        super().__init__(f"HTTP {code}: {body_text}")
        self.code = code
        self.body_text = body_text


def http_json(method: str, url: str, *, payload=None, headers=None, timeout: int = 20):
    data = None
    req_headers = {"Content-Type": "application/json"}
    if headers:
        req_headers.update(headers)

    if payload is not None:
        data = json.dumps(payload).encode("utf-8")

    req = urllib.request.Request(url, data=data, headers=req_headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw) if raw else {}, dict(resp.headers.items())
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise HTTPJSONError(exc.code, body) from exc


def create_session(*, pds_url: str, handle: str, app_password: str, timeout: int) -> SessionState:
    payload = {
        "identifier": handle,
        "password": app_password,
    }
    data, _ = http_json(
        "POST",
        f"{pds_url.rstrip('/')}/xrpc/com.atproto.server.createSession",
        payload=payload,
        timeout=timeout,
    )
    return SessionState(access_jwt=data["accessJwt"])


def fetch_post_thread(
    *,
    appview_url: str,
    uri: str,
    access_jwt: str,
    timeout: int,
    forced_labeler_did: str | None = None,
):
    query = urllib.parse.urlencode(
        {
            "uri": uri,
            "depth": 0,
            "parentHeight": 0,
        }
    )
    headers = {
        "Authorization": f"Bearer {access_jwt}",
    }
    if forced_labeler_did:
        headers["atproto-accept-labelers"] = forced_labeler_did

    return http_json(
        "GET",
        f"{appview_url.rstrip('/')}/xrpc/app.bsky.feed.getPostThread?{query}",
        headers=headers,
        timeout=timeout,
    )


def payload_has_label(*, payload: dict, uri: str, label_value: str, labeler_did: str) -> bool:
    thread = payload.get("thread") or {}
    post = thread.get("post") or {}
    if post.get("uri") != uri:
        return False

    labels = post.get("labels") or []
    for label in labels:
        if label.get("val") == label_value and label.get("src") == labeler_did:
            return True
    return False


def is_expired_token_error(exc: HTTPJSONError) -> bool:
    if exc.code == 401:
        return True

    if exc.code != 400:
        return False

    try:
        body = json.loads(exc.body_text)
    except Exception:
        return False

    return isinstance(body, dict) and body.get("error") == "ExpiredToken"


def fetch_post_thread_with_session_refresh(
    *,
    session_state: SessionState,
    appview_url: str,
    uri: str,
    timeout: int,
    forced_labeler_did: str | None,
    pds_url: str,
    handle: str,
    app_password: str,
) -> tuple[dict, dict[str, str], SessionState]:
    try:
        payload, headers = fetch_post_thread(
            appview_url=appview_url,
            uri=uri,
            access_jwt=session_state.access_jwt,
            timeout=timeout,
            forced_labeler_did=forced_labeler_did,
        )
        return payload, headers, session_state
    except HTTPJSONError as exc:
        if not is_expired_token_error(exc):
            raise

        refreshed = create_session(
            pds_url=pds_url,
            handle=handle,
            app_password=app_password,
            timeout=timeout,
        )

        payload, headers = fetch_post_thread(
            appview_url=appview_url,
            uri=uri,
            access_jwt=refreshed.access_jwt,
            timeout=timeout,
            forced_labeler_did=forced_labeler_did,
        )
        return payload, headers, refreshed


def seed_visibility_rows(*, lookback_hours: int, label_missing_alt: str, label_partial_alt: str) -> int:
    with engine.begin() as conn:
        result = conn.execute(
            text(
                """
                INSERT INTO label_visibility (
                    uri,
                    cid,
                    label_value,
                    first_published_at,
                    record_created_at,
                    created_at,
                    updated_at
                )
                SELECT
                    lp.uri,
                    lp.cid,
                    lp.label_value,
                    lp.published_at,
                    NULLIF(pe.record_created_at, '')::timestamptz,
                    NOW(),
                    NOW()
                FROM label_publication lp
                LEFT JOIN post_evaluation pe
                  ON pe.uri = lp.uri
                 AND pe.cid = lp.cid
                 AND pe.derived_label = lp.label_value
                WHERE lp.status = 'published'
                  AND lp.label_value IN (:missing_label, :partial_label)
                  AND lp.published_at >= NOW() - (:lookback_hours * INTERVAL '1 hour')
                ON CONFLICT (uri, cid, label_value) DO UPDATE
                SET
                    first_published_at = COALESCE(label_visibility.first_published_at, EXCLUDED.first_published_at),
                    record_created_at = COALESCE(label_visibility.record_created_at, EXCLUDED.record_created_at),
                    updated_at = NOW()
                """
            ),
            {
                "missing_label": label_missing_alt,
                "partial_label": label_partial_alt,
                "lookback_hours": lookback_hours,
            },
        )
        return result.rowcount or 0


def load_candidates(*, batch_size: int):
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT
                    uri,
                    cid,
                    label_value,
                    first_published_at,
                    record_created_at,
                    forced_visible_at,
                    subscriber_visible_at
                FROM label_visibility
                WHERE forced_visible_at IS NULL
                   OR subscriber_visible_at IS NULL
                ORDER BY first_published_at DESC NULLS LAST, id DESC
                LIMIT :batch_size
                """
            ),
            {"batch_size": batch_size},
        ).mappings().all()
    return [dict(row) for row in rows]


def mark_forced_result(*, uri: str, cid: str, label_value: str, visible: bool, error_text: str | None):
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE label_visibility
                SET
                    last_forced_checked_at = NOW(),
                    forced_check_count = forced_check_count + 1,
                    last_forced_visible = :visible,
                    forced_visible_at = CASE
                        WHEN :visible AND forced_visible_at IS NULL THEN NOW()
                        ELSE forced_visible_at
                    END,
                    last_forced_error = :error_text,
                    updated_at = NOW()
                WHERE uri = :uri AND cid = :cid AND label_value = :label_value
                """
            ),
            {
                "uri": uri,
                "cid": cid,
                "label_value": label_value,
                "visible": visible,
                "error_text": error_text,
            },
        )


def mark_subscriber_result(*, uri: str, cid: str, label_value: str, visible: bool, error_text: str | None):
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE label_visibility
                SET
                    last_subscriber_checked_at = NOW(),
                    subscriber_check_count = subscriber_check_count + 1,
                    last_subscriber_visible = :visible,
                    subscriber_visible_at = CASE
                        WHEN :visible AND subscriber_visible_at IS NULL THEN NOW()
                        ELSE subscriber_visible_at
                    END,
                    last_subscriber_error = :error_text,
                    updated_at = NOW()
                WHERE uri = :uri AND cid = :cid AND label_value = :label_value
                """
            ),
            {
                "uri": uri,
                "cid": cid,
                "label_value": label_value,
                "visible": visible,
                "error_text": error_text,
            },
        )


def main() -> None:
    app_settings = get_settings()
    verifier_settings = VerifierSettings()

    configure_logging(app_settings.log_level)

    if not app_settings.test_viewer_handle or not app_settings.test_viewer_app_password:
        raise RuntimeError(
            "TEST_VIEWER_HANDLE and TEST_VIEWER_APP_PASSWORD must be set in .env for the visibility verifier"
        )

    logger.info(
        "label_visibility_verifier_starting",
        extra={
            "labeler_did": verifier_settings.verifier_labeler_did,
            "appview_url": verifier_settings.verifier_appview_url,
            "batch_size": verifier_settings.verifier_batch_size,
            "sleep_seconds": verifier_settings.verifier_sleep_seconds,
            "lookback_hours": verifier_settings.verifier_lookback_hours,
        },
    )

    session_state = create_session(
        pds_url=app_settings.bsky_pds_url,
        handle=app_settings.test_viewer_handle,
        app_password=app_settings.test_viewer_app_password,
        timeout=verifier_settings.verifier_request_timeout_seconds,
    )

    while True:
        try:
            inserted = seed_visibility_rows(
                lookback_hours=verifier_settings.verifier_lookback_hours,
                label_missing_alt=app_settings.label_missing_alt,
                label_partial_alt=app_settings.label_partial_alt,
            )
            if inserted:
                logger.info("label_visibility_seeded", extra={"rows": inserted})

            candidates = load_candidates(batch_size=verifier_settings.verifier_batch_size)

            if not candidates:
                logger.info("label_visibility_no_candidates")
                time.sleep(verifier_settings.verifier_sleep_seconds)
                continue

            forced_found = 0
            subscriber_found = 0

            for row in candidates:
                uri = row["uri"]
                cid = row["cid"]
                label_value = row["label_value"]

                if row["forced_visible_at"] is None:
                    visible = False
                    error_text = None
                    try:
                        payload, _headers, session_state = fetch_post_thread_with_session_refresh(
                            session_state=session_state,
                            appview_url=verifier_settings.verifier_appview_url,
                            uri=uri,
                            timeout=verifier_settings.verifier_request_timeout_seconds,
                            forced_labeler_did=verifier_settings.verifier_labeler_did,
                            pds_url=app_settings.bsky_pds_url,
                            handle=app_settings.test_viewer_handle,
                            app_password=app_settings.test_viewer_app_password,
                        )
                        visible = payload_has_label(
                            payload=payload,
                            uri=uri,
                            label_value=label_value,
                            labeler_did=verifier_settings.verifier_labeler_did,
                        )
                    except HTTPJSONError as exc:
                        error_text = f"{exc.code}: {exc.body_text}"
                    except Exception as exc:
                        error_text = str(exc)

                    mark_forced_result(
                        uri=uri,
                        cid=cid,
                        label_value=label_value,
                        visible=visible,
                        error_text=error_text,
                    )
                    if visible:
                        forced_found += 1

                if row["subscriber_visible_at"] is None:
                    visible = False
                    error_text = None
                    try:
                        payload, _headers, session_state = fetch_post_thread_with_session_refresh(
                            session_state=session_state,
                            appview_url=verifier_settings.verifier_appview_url,
                            uri=uri,
                            timeout=verifier_settings.verifier_request_timeout_seconds,
                            forced_labeler_did=None,
                            pds_url=app_settings.bsky_pds_url,
                            handle=app_settings.test_viewer_handle,
                            app_password=app_settings.test_viewer_app_password,
                        )
                        visible = payload_has_label(
                            payload=payload,
                            uri=uri,
                            label_value=label_value,
                            labeler_did=verifier_settings.verifier_labeler_did,
                        )
                    except HTTPJSONError as exc:
                        error_text = f"{exc.code}: {exc.body_text}"
                    except Exception as exc:
                        error_text = str(exc)

                    mark_subscriber_result(
                        uri=uri,
                        cid=cid,
                        label_value=label_value,
                        visible=visible,
                        error_text=error_text,
                    )
                    if visible:
                        subscriber_found += 1

            logger.info(
                "label_visibility_batch_complete",
                extra={
                    "candidate_count": len(candidates),
                    "forced_found": forced_found,
                    "subscriber_found": subscriber_found,
                },
            )

        except Exception:
            logger.exception("label_visibility_verifier_loop_failed")

        time.sleep(verifier_settings.verifier_sleep_seconds)


if __name__ == "__main__":
    main()