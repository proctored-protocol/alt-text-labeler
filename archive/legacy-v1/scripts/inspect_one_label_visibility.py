import argparse
import json
import urllib.parse
import urllib.request
from typing import Any

from sqlalchemy import text

from app.config import get_settings
from app.db import engine


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
            return (
                json.loads(raw) if raw else {},
                dict(resp.headers.items()),
            )
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise HTTPJSONError(exc.code, body) from exc


def create_session(*, pds_url: str, handle: str, app_password: str, timeout: int):
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
    return data["accessJwt"]


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
        headers["atproto-content-labelers"] = forced_labeler_did

    return http_json(
        "GET",
        f"{appview_url.rstrip('/')}/xrpc/app.bsky.feed.getPostThread?{query}",
        headers=headers,
        timeout=timeout,
    )


def summarize_payload(payload: dict[str, Any], *, target_uri: str, target_label: str, target_src: str):
    thread = payload.get("thread") or {}
    post = thread.get("post") or {}

    out = {
        "post_uri": post.get("uri"),
        "author_did": ((post.get("author") or {}).get("did")),
        "labels": post.get("labels") or [],
        "author_labels": ((post.get("author") or {}).get("labels") or []),
        "matches_target_post": post.get("uri") == target_uri,
        "has_target_label": False,
    }

    for label in out["labels"]:
        if label.get("val") == target_label and label.get("src") == target_src:
            out["has_target_label"] = True
            break

    return out


def load_recent_candidate(*, minutes: int):
    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT
                    lp.uri,
                    lp.cid,
                    lp.label_value,
                    lp.published_at,
                    pe.record_created_at
                FROM label_publication lp
                LEFT JOIN post_evaluation pe
                  ON pe.uri = lp.uri
                 AND pe.cid = lp.cid
                 AND pe.derived_label = lp.label_value
                WHERE lp.status = 'published'
                  AND lp.published_at >= NOW() - (:minutes * INTERVAL '1 minute')
                ORDER BY lp.published_at DESC
                LIMIT 1
                """
            ),
            {"minutes": minutes},
        ).mappings().first()

    if row is None:
        return None
    return dict(row)


def load_specific_candidate(*, uri: str, cid: str, label_value: str):
    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT
                    lp.uri,
                    lp.cid,
                    lp.label_value,
                    lp.published_at,
                    pe.record_created_at
                FROM label_publication lp
                LEFT JOIN post_evaluation pe
                  ON pe.uri = lp.uri
                 AND pe.cid = lp.cid
                 AND pe.derived_label = lp.label_value
                WHERE lp.uri = :uri
                  AND lp.cid = :cid
                  AND lp.label_value = :label_value
                ORDER BY lp.published_at DESC
                LIMIT 1
                """
            ),
            {
                "uri": uri,
                "cid": cid,
                "label_value": label_value,
            },
        ).mappings().first()

    if row is None:
        return None
    return dict(row)


def main() -> None:
    parser = argparse.ArgumentParser(description="Inspect one fresh published label end-to-end")
    parser.add_argument("--minutes", type=int, default=30, help="Look back this many minutes for a recent published label")
    parser.add_argument("--uri", default=None)
    parser.add_argument("--cid", default=None)
    parser.add_argument("--label-value", default=None)
    args = parser.parse_args()

    settings = get_settings()

    if not settings.test_viewer_handle or not settings.test_viewer_app_password:
        raise RuntimeError("TEST_VIEWER_HANDLE and TEST_VIEWER_APP_PASSWORD must be set in .env")

    verifier_labeler_did = None
    with open(".env", "r", encoding="utf-8") as f:
        for line in f:
            if line.startswith("VERIFIER_LABELER_DID="):
                verifier_labeler_did = line.split("=", 1)[1].strip()
                break

    if not verifier_labeler_did:
        raise RuntimeError("VERIFIER_LABELER_DID must be set in .env")

    if args.uri and args.cid and args.label_value:
        row = load_specific_candidate(uri=args.uri, cid=args.cid, label_value=args.label_value)
    else:
        row = load_recent_candidate(minutes=args.minutes)

    if row is None:
        print("No matching published label row found.")
        return

    print("=== selected published label row ===")
    print(json.dumps(row, default=str, indent=2))

    access_jwt = create_session(
        pds_url=settings.bsky_pds_url,
        handle=settings.test_viewer_handle,
        app_password=settings.test_viewer_app_password,
        timeout=20,
    )

    print("\n=== forced fetch ===")
    try:
        payload, headers = fetch_post_thread(
            appview_url="https://bsky.social",
            uri=row["uri"],
            access_jwt=access_jwt,
            timeout=20,
            forced_labeler_did=verifier_labeler_did,
        )
        print("response_headers:", json.dumps({
            "atproto-content-labelers": headers.get("atproto-content-labelers"),
        }, indent=2))
        print(json.dumps(
            summarize_payload(
                payload,
                target_uri=row["uri"],
                target_label=row["label_value"],
                target_src=verifier_labeler_did,
            ),
            indent=2,
        ))
    except HTTPJSONError as exc:
        print(f"forced fetch error: HTTP {exc.code} {exc.body_text}")
    except Exception as exc:
        print(f"forced fetch error: {exc}")

    print("\n=== subscriber-normal fetch ===")
    try:
        payload, headers = fetch_post_thread(
            appview_url="https://bsky.social",
            uri=row["uri"],
            access_jwt=access_jwt,
            timeout=20,
            forced_labeler_did=None,
        )
        print("response_headers:", json.dumps({
            "atproto-content-labelers": headers.get("atproto-content-labelers"),
        }, indent=2))
        print(json.dumps(
            summarize_payload(
                payload,
                target_uri=row["uri"],
                target_label=row["label_value"],
                target_src=verifier_labeler_did,
            ),
            indent=2,
        ))
    except HTTPJSONError as exc:
        print(f"subscriber fetch error: HTTP {exc.code} {exc.body_text}")
    except Exception as exc:
        print(f"subscriber fetch error: {exc}")

    print("\n=== declaration spot-check commands ===")
    print(
        f'curl -s "https://public.api.bsky.app/xrpc/app.bsky.labeler.getServices?dids={verifier_labeler_did}&detailed=true"'
    )
    print(
        f'curl -s "https://bsky.social/xrpc/com.atproto.repo.getRecord?repo={verifier_labeler_did}&collection=app.bsky.labeler.service&rkey=self"'
    )


if __name__ == "__main__":
    main()