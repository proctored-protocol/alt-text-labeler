from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen
from urllib.error import HTTPError

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from app.config import get_settings
from app.integrations.ozone.auth import clear_test_viewer_access_jwt_cache, get_test_viewer_access_jwt
from app.integrations.ozone.client import ozone_post


PUBLIC_API_BASE_URL = "https://public.api.bsky.app"


class ScriptSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    verifier_labeler_did: str = Field(...)
    verifier_appview_url: str = Field(default="https://bsky.social")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_now() -> str:
    return utc_now().isoformat()


class HTTPJSONError(RuntimeError):
    def __init__(self, code: int, body_text: str, payload: Any | None = None):
        super().__init__(f"HTTP {code}: {body_text}")
        self.code = code
        self.body_text = body_text
        self.payload = payload


@dataclass
class ResolvedPost:
    post_url: str
    profile_token: str
    rkey: str
    did: str
    at_uri: str
    cid: str


@dataclass
class VisibilityResult:
    ok: bool
    status_code: int
    found_label: bool
    response_headers: dict[str, str]
    payload: Any
    error_text: str | None = None


@dataclass
class VerificationSnapshot:
    query_labels: VisibilityResult
    forced_hydration: VisibilityResult
    subscriber_hydration: VisibilityResult


def http_json(
    url: str,
    *,
    method: str = "GET",
    payload: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    timeout: int = 30,
) -> tuple[int, dict[str, str], Any]:
    body = None
    req_headers = {"Accept": "application/json"}
    if headers:
        req_headers.update(headers)

    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        req_headers["Content-Type"] = "application/json"

    req = Request(url, data=body, headers=req_headers, method=method)

    try:
        with urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
            data = json.loads(raw) if raw else {}
            return resp.status, dict(resp.headers.items()), data
    except HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        try:
            data = json.loads(raw) if raw else {}
        except Exception:
            data = {"raw": raw}
        raise HTTPJSONError(exc.code, raw, payload=data) from exc


def parse_post_url(post_url: str) -> tuple[str, str]:
    parsed = urlparse(post_url)
    if parsed.scheme != "https" or parsed.netloc != "bsky.app":
        raise ValueError("Post URL must be an https://bsky.app/profile/.../post/... link")

    parts = [p for p in parsed.path.split("/") if p]
    if len(parts) != 4 or parts[0] != "profile" or parts[2] != "post":
        raise ValueError("Post URL must look like https://bsky.app/profile/<handle-or-did>/post/<rkey>")

    profile_token = parts[1]
    rkey = parts[3]
    if not profile_token or not rkey:
        raise ValueError("Could not extract profile token and post rkey from post URL")

    return profile_token, rkey


def resolve_profile_token_to_did(profile_token: str, *, timeout: int) -> str:
    if profile_token.startswith("did:"):
        return profile_token

    params = urlencode({"handle": profile_token})
    url = f"{PUBLIC_API_BASE_URL}/xrpc/com.atproto.identity.resolveHandle?{params}"
    _status, _headers, payload = http_json(url, timeout=timeout)
    did = payload.get("did")
    if not did:
        raise RuntimeError(f"resolveHandle did not return a DID for {profile_token}")
    return did


def fetch_post_cid(at_uri: str, *, timeout: int) -> str:
    params = urlencode([("uris", at_uri)])
    url = f"{PUBLIC_API_BASE_URL}/xrpc/app.bsky.feed.getPosts?{params}"
    _status, _headers, payload = http_json(url, timeout=timeout)
    posts = payload.get("posts") or []
    if not posts:
        raise RuntimeError(f"getPosts returned no posts for {at_uri}")

    post = posts[0]
    cid = post.get("cid")
    if not cid:
        raise RuntimeError(f"getPosts did not return a CID for {at_uri}")
    return cid


def resolve_post(post_url: str, *, timeout: int) -> ResolvedPost:
    profile_token, rkey = parse_post_url(post_url)
    did = resolve_profile_token_to_did(profile_token, timeout=timeout)
    at_uri = f"at://{did}/app.bsky.feed.post/{rkey}"
    cid = fetch_post_cid(at_uri, timeout=timeout)
    return ResolvedPost(
        post_url=post_url,
        profile_token=profile_token,
        rkey=rkey,
        did=did,
        at_uri=at_uri,
        cid=cid,
    )


def created_by_did() -> str:
    settings = get_settings()
    if not settings.ozone_proxy_did:
        raise RuntimeError("OZONE_PROXY_DID must be set in .env")
    return settings.ozone_proxy_did.split("#", 1)[0]


def build_emit_event_payload(*, at_uri: str, cid: str, label_value: str) -> dict[str, Any]:
    return {
        "event": {
            "$type": "tools.ozone.moderation.defs#modEventLabel",
            "createLabelVals": [label_value],
            "negateLabelVals": [],
            "comment": f"Manual publish test via alt-labeler at {iso_now()}",
        },
        "subject": {
            "$type": "com.atproto.repo.strongRef",
            "uri": at_uri,
            "cid": cid,
        },
        "createdBy": created_by_did(),
    }


def publish_label_via_ozone(*, at_uri: str, cid: str, label_value: str) -> dict[str, Any]:
    payload = build_emit_event_payload(at_uri=at_uri, cid=cid, label_value=label_value)
    return ozone_post("tools.ozone.moderation.emitEvent", payload)


def get_viewer_access_jwt(*, force_refresh: bool = False) -> str:
    if force_refresh:
        clear_test_viewer_access_jwt_cache()

    token = get_test_viewer_access_jwt()
    if not token:
        raise RuntimeError(
            "TEST_VIEWER_HANDLE and TEST_VIEWER_APP_PASSWORD must be set in .env for verification"
        )
    return token


def is_expired_token_error(exc: HTTPJSONError) -> bool:
    if exc.code == 401:
        return True
    if exc.code != 400:
        return False
    if isinstance(exc.payload, dict) and exc.payload.get("error") == "ExpiredToken":
        return True
    return False


def query_labels(*, at_uri: str, labeler_did: str, timeout: int) -> VisibilityResult:
    params = urlencode(
        [
            ("uriPatterns", at_uri),
            ("sources", labeler_did),
            ("limit", "20"),
        ]
    )
    url = f"{PUBLIC_API_BASE_URL}/xrpc/com.atproto.label.queryLabels?{params}"

    try:
        status, headers, payload = http_json(url, timeout=timeout)
        labels = payload.get("labels") or []
        found = any(
            lbl.get("src") == labeler_did and lbl.get("uri") == at_uri
            for lbl in labels
        )
        return VisibilityResult(
            ok=True,
            status_code=status,
            found_label=found,
            response_headers=headers,
            payload=payload,
            error_text=None,
        )
    except HTTPJSONError as exc:
        return VisibilityResult(
            ok=False,
            status_code=exc.code,
            found_label=False,
            response_headers={},
            payload=exc.payload,
            error_text=f"{exc.code}: {exc.body_text}",
        )


def get_post_thread_visibility(
    *,
    at_uri: str,
    label_value: str,
    labeler_did: str,
    timeout: int,
    forced: bool,
) -> VisibilityResult:
    script_settings = ScriptSettings()
    params = urlencode(
        [
            ("uri", at_uri),
            ("depth", "0"),
            ("parentHeight", "0"),
        ]
    )
    url = f"{script_settings.verifier_appview_url.rstrip('/')}/xrpc/app.bsky.feed.getPostThread?{params}"

    token = get_viewer_access_jwt(force_refresh=False)
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }
    if forced:
        headers["atproto-accept-labelers"] = labeler_did

    try:
        status, resp_headers, payload = http_json(url, headers=headers, timeout=timeout)
    except HTTPJSONError as exc:
        if is_expired_token_error(exc):
            token = get_viewer_access_jwt(force_refresh=True)
            headers["Authorization"] = f"Bearer {token}"
            status, resp_headers, payload = http_json(url, headers=headers, timeout=timeout)
        else:
            return VisibilityResult(
                ok=False,
                status_code=exc.code,
                found_label=False,
                response_headers={},
                payload=exc.payload,
                error_text=f"{exc.code}: {exc.body_text}",
            )

    thread = payload.get("thread") or {}
    post = thread.get("post") or {}
    labels = post.get("labels") or []
    found = any(
        lbl.get("src") == labeler_did
        and lbl.get("uri") == at_uri
        and lbl.get("val") == label_value
        for lbl in labels
    )

    return VisibilityResult(
        ok=True,
        status_code=status,
        found_label=found,
        response_headers=resp_headers,
        payload=payload,
        error_text=None,
    )


def verify_once(
    *,
    at_uri: str,
    label_value: str,
    labeler_did: str,
    timeout: int,
    skip_forced_check: bool,
    skip_subscriber_check: bool,
) -> VerificationSnapshot:
    ql = query_labels(at_uri=at_uri, labeler_did=labeler_did, timeout=timeout)

    if skip_forced_check:
        forced = VisibilityResult(
            ok=True,
            status_code=0,
            found_label=False,
            response_headers={},
            payload={"skipped": True},
            error_text=None,
        )
    else:
        forced = get_post_thread_visibility(
            at_uri=at_uri,
            label_value=label_value,
            labeler_did=labeler_did,
            timeout=timeout,
            forced=True,
        )

    if skip_subscriber_check:
        subscriber = VisibilityResult(
            ok=True,
            status_code=0,
            found_label=False,
            response_headers={},
            payload={"skipped": True},
            error_text=None,
        )
    else:
        subscriber = get_post_thread_visibility(
            at_uri=at_uri,
            label_value=label_value,
            labeler_did=labeler_did,
            timeout=timeout,
            forced=False,
        )

    return VerificationSnapshot(
        query_labels=ql,
        forced_hydration=forced,
        subscriber_hydration=subscriber,
    )


def verification_succeeded(
    snapshot: VerificationSnapshot,
    *,
    require_forced: bool,
    require_subscriber: bool,
) -> bool:
    forced_ok = True if not require_forced else snapshot.forced_hydration.found_label
    subscriber_ok = True if not require_subscriber else snapshot.subscriber_hydration.found_label
    return snapshot.query_labels.found_label and forced_ok and subscriber_ok


def summarize_snapshot(snapshot: VerificationSnapshot) -> dict[str, Any]:
    return {
        "query_labels": {
            "ok": snapshot.query_labels.ok,
            "status_code": snapshot.query_labels.status_code,
            "found_label": snapshot.query_labels.found_label,
            "error_text": snapshot.query_labels.error_text,
        },
        "forced_hydration": {
            "ok": snapshot.forced_hydration.ok,
            "status_code": snapshot.forced_hydration.status_code,
            "found_label": snapshot.forced_hydration.found_label,
            "error_text": snapshot.forced_hydration.error_text,
            "atproto_content_labelers": snapshot.forced_hydration.response_headers.get("atproto-content-labelers"),
        },
        "subscriber_hydration": {
            "ok": snapshot.subscriber_hydration.ok,
            "status_code": snapshot.subscriber_hydration.status_code,
            "found_label": snapshot.subscriber_hydration.found_label,
            "error_text": snapshot.subscriber_hydration.error_text,
            "atproto_content_labelers": snapshot.subscriber_hydration.response_headers.get("atproto-content-labelers"),
        },
    }


def print_json(data: Any) -> None:
    print(json.dumps(data, indent=2, ensure_ascii=False, default=str))


def print_summary(
    *,
    resolved: ResolvedPost,
    label_value: str,
    ozone_response: dict[str, Any],
    attempts: list[dict[str, Any]],
    total_seconds: float,
) -> None:
    print()
    print("=== manual publish and verify summary ===")
    print(f"post_url:       {resolved.post_url}")
    print(f"profile_token:  {resolved.profile_token}")
    print(f"did:            {resolved.did}")
    print(f"rkey:           {resolved.rkey}")
    print(f"at_uri:         {resolved.at_uri}")
    print(f"cid:            {resolved.cid}")
    print(f"label_value:    {label_value}")
    print(f"total_seconds:  {round(total_seconds, 2)}")
    print()
    print("ozone_response:")
    print_json(ozone_response)
    print()
    print("verification_attempts:")
    print_json(attempts)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Publish one label via Ozone for a Bluesky HTTPS post URL and verify visibility."
    )
    parser.add_argument(
        "--post-url",
        required=True,
        help="Bluesky HTTPS post URL, e.g. https://bsky.app/profile/<handle-or-did>/post/<rkey>",
    )
    parser.add_argument(
        "--label-value",
        required=True,
        help="Label value to apply, e.g. missing-alt-text or partial-alt-text",
    )
    parser.add_argument(
        "--verify-timeout-seconds",
        type=int,
        default=180,
        help="How long to keep retrying visibility checks after Ozone submission.",
    )
    parser.add_argument(
        "--verify-interval-seconds",
        type=int,
        default=5,
        help="Seconds between visibility checks.",
    )
    parser.add_argument(
        "--request-timeout-seconds",
        type=int,
        default=30,
        help="HTTP timeout for network requests.",
    )
    parser.add_argument(
        "--skip-forced-check",
        action="store_true",
        help="Skip forced hydration verification.",
    )
    parser.add_argument(
        "--skip-subscriber-check",
        action="store_true",
        help="Skip subscriber hydration verification.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print final output as JSON only.",
    )
    args = parser.parse_args()

    script_settings = ScriptSettings()
    labeler_did = script_settings.verifier_labeler_did

    started_at = time.monotonic()
    resolved = resolve_post(args.post_url, timeout=args.request_timeout_seconds)
    ozone_response = publish_label_via_ozone(
        at_uri=resolved.at_uri,
        cid=resolved.cid,
        label_value=args.label_value,
    )

    require_forced = not args.skip_forced_check
    require_subscriber = not args.skip_subscriber_check

    attempts: list[dict[str, Any]] = []
    deadline = time.monotonic() + args.verify_timeout_seconds

    attempt_no = 0
    while True:
        attempt_no += 1
        snapshot = verify_once(
            at_uri=resolved.at_uri,
            label_value=args.label_value,
            labeler_did=labeler_did,
            timeout=args.request_timeout_seconds,
            skip_forced_check=args.skip_forced_check,
            skip_subscriber_check=args.skip_subscriber_check,
        )
        attempt_entry = {
            "attempt": attempt_no,
            "checked_at": iso_now(),
            "summary": summarize_snapshot(snapshot),
        }
        attempts.append(attempt_entry)

        if verification_succeeded(
            snapshot,
            require_forced=require_forced,
            require_subscriber=require_subscriber,
        ):
            total_seconds = time.monotonic() - started_at
            result = {
                "success": True,
                "resolved_post": asdict(resolved),
                "label_value": args.label_value,
                "ozone_response": ozone_response,
                "verification_attempts": attempts,
                "total_seconds": round(total_seconds, 2),
            }
            if args.json:
                print_json(result)
            else:
                print_summary(
                    resolved=resolved,
                    label_value=args.label_value,
                    ozone_response=ozone_response,
                    attempts=attempts,
                    total_seconds=total_seconds,
                )
            raise SystemExit(0)

        if time.monotonic() >= deadline:
            break

        time.sleep(args.verify_interval_seconds)

    total_seconds = time.monotonic() - started_at
    result = {
        "success": False,
        "resolved_post": asdict(resolved),
        "label_value": args.label_value,
        "ozone_response": ozone_response,
        "verification_attempts": attempts,
        "total_seconds": round(total_seconds, 2),
    }

    if args.json:
        print_json(result)
    else:
        print_summary(
            resolved=resolved,
            label_value=args.label_value,
            ozone_response=ozone_response,
            attempts=attempts,
            total_seconds=total_seconds,
        )

    raise SystemExit(2)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise