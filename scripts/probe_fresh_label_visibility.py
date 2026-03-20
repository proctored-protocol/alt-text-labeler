from __future__ import annotations

import argparse
import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from urllib.error import HTTPError

from sqlalchemy import text

from app.config import get_settings
from app.db import engine


LABELER_DID = "did:plc:rh3vjqs4npfpmnkkmx4u4bzj"


@dataclass
class Sample:
    uri: str
    cid: Optional[str]
    label_value: Optional[str]
    published_at: Optional[datetime]


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_or_none(dt: Optional[datetime]) -> Optional[str]:
    return dt.isoformat() if dt is not None else None


def uri_to_web_link(uri: str) -> str:
    parts = uri.split("/")
    if len(parts) < 5:
        return uri
    did = parts[2]
    rkey = parts[-1]
    return f"https://bsky.app/profile/{did}/post/{rkey}"


def http_json(
    url: str,
    *,
    headers: Optional[dict[str, str]] = None,
) -> tuple[int, dict[str, str], Any]:
    req = Request(url, headers=headers or {}, method="GET")
    try:
        with urlopen(req, timeout=30) as resp:
            body = resp.read().decode("utf-8")
            return resp.status, dict(resp.headers.items()), json.loads(body)
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        try:
            payload = json.loads(body)
        except Exception:
            payload = {"raw": body}
        return exc.code, {}, payload


def get_test_viewer_access_jwt() -> str:
    settings = get_settings()
    payload = json.dumps(
        {
            "identifier": settings.test_viewer_handle,
            "password": settings.test_viewer_app_password,
        }
    ).encode("utf-8")

    req = Request(
        f"{settings.bsky_pds_url.rstrip('/')}/xrpc/com.atproto.server.createSession",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    return data["accessJwt"]


def select_samples(within_minutes: int, limit: int) -> list[Sample]:
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT uri, cid, label_value, published_at
                FROM label_publication
                WHERE status = 'published'
                  AND published_at >= NOW() - (:minutes || ' minutes')::interval
                ORDER BY published_at DESC
                LIMIT :limit
                """
            ),
            {"minutes": within_minutes, "limit": limit},
        ).mappings().all()

    out: list[Sample] = []
    for row in rows:
        out.append(
            Sample(
                uri=row["uri"],
                cid=row.get("cid"),
                label_value=row.get("label_value"),
                published_at=row.get("published_at"),
            )
        )
    return out


def lookup_sample_by_uri(uri: str) -> Sample:
    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT uri, cid, label_value, published_at
                FROM label_publication
                WHERE uri = :uri
                  AND status = 'published'
                ORDER BY published_at DESC
                LIMIT 1
                """
            ),
            {"uri": uri},
        ).mappings().first()

    if row is None:
        return Sample(uri=uri, cid=None, label_value=None, published_at=None)

    return Sample(
        uri=row["uri"],
        cid=row.get("cid"),
        label_value=row.get("label_value"),
        published_at=row.get("published_at"),
    )


def query_labels(uri: str) -> tuple[bool, Any]:
    params = urlencode(
        [
            ("uriPatterns", uri),
            ("sources", LABELER_DID),
            ("limit", "20"),
        ]
    )
    status, _, payload = http_json(
        f"https://public.api.bsky.app/xrpc/com.atproto.label.queryLabels?{params}"
    )
    if status != 200:
        return False, payload

    labels = payload.get("labels", []) or []
    found = any(
        lbl.get("src") == LABELER_DID and lbl.get("uri") == uri
        for lbl in labels
    )
    return found, payload


def get_post_thread(
    uri: str,
    *,
    access_jwt: str,
    forced: bool,
) -> tuple[bool, Any, dict[str, str], int]:
    headers = {
        "Authorization": f"Bearer {access_jwt}",
    }
    if forced:
        headers["atproto-accept-labelers"] = LABELER_DID

    params = urlencode(
        [
            ("uri", uri),
            ("depth", "0"),
            ("parentHeight", "0"),
        ]
    )
    status, resp_headers, payload = http_json(
        f"https://bsky.social/xrpc/app.bsky.feed.getPostThread?{params}",
        headers=headers,
    )

    if status != 200:
        return False, payload, resp_headers, status

    post = ((payload.get("thread") or {}).get("post") or {})
    labels = post.get("labels") or []
    found = any(
        lbl.get("src") == LABELER_DID and lbl.get("uri") == uri
        for lbl in labels
    )
    return found, payload, resp_headers, status


def print_probe(sample: Sample, access_jwt: str) -> None:
    now = utc_now()

    ql_found, ql_payload = query_labels(sample.uri)
    forced_found, forced_payload, forced_headers, forced_status = get_post_thread(
        sample.uri,
        access_jwt=access_jwt,
        forced=True,
    )
    normal_found, normal_payload, normal_headers, normal_status = get_post_thread(
        sample.uri,
        access_jwt=access_jwt,
        forced=False,
    )

    if sample.published_at is not None:
        age_s = round((now - sample.published_at).total_seconds(), 1)
    else:
        age_s = None

    print("------------------------------------------------------------")
    print(f"uri:                {sample.uri}")
    print(f"link:               {uri_to_web_link(sample.uri)}")
    print(f"cid:                {sample.cid}")
    print(f"label_value:        {sample.label_value}")
    print(f"published_at:       {iso_or_none(sample.published_at)}")
    print(f"age_since_publish:  {age_s}s" if age_s is not None else "age_since_publish:  unknown")
    print(f"queryLabels_found:  {ql_found}")
    print(f"forced_status:      {forced_status}")
    print(f"forced_visible:     {forced_found}")
    print(f"forced_header:      {forced_headers.get('atproto-content-labelers')}")
    print(f"normal_status:      {normal_status}")
    print(f"normal_visible:     {normal_found}")
    print(f"normal_header:      {normal_headers.get('atproto-content-labelers')}")

    if not ql_found:
        print("queryLabels_payload:", json.dumps(ql_payload, ensure_ascii=False)[:600])

    if forced_status != 200:
        print("forced_payload:", json.dumps(forced_payload, ensure_ascii=False)[:600])

    if normal_status != 200:
        print("normal_payload:", json.dumps(normal_payload, ensure_ascii=False)[:600])


def main() -> None:
    parser = argparse.ArgumentParser(description="Probe label visibility for fresh or specific published labels.")
    parser.add_argument("--uri", help="Specific at:// URI to probe.")
    parser.add_argument("--within-minutes", type=int, default=15, help="Look back this many minutes for fresh published labels.")
    parser.add_argument("--limit", type=int, default=10, help="How many fresh samples to probe when --uri is not set.")
    parser.add_argument("--repeat", type=int, default=1, help="How many times to repeat the probe.")
    parser.add_argument("--sleep-seconds", type=int, default=300, help="Sleep between repeated probe runs.")
    args = parser.parse_args()

    access_jwt = get_test_viewer_access_jwt()

    for idx in range(args.repeat):
        print()
        print("=== fresh label visibility probe ===")
        print(f"utc_now: {utc_now().isoformat()}")
        print(f"labeler_did: {LABELER_DID}")
        print()

        if args.uri:
            sample = lookup_sample_by_uri(args.uri)
            print_probe(sample, access_jwt)
        else:
            samples = select_samples(args.within_minutes, args.limit)
            print(f"within_minutes: {args.within_minutes}")
            print(f"sample_count:   {len(samples)}")
            print()
            if not samples:
                print("no fresh published samples found")
            else:
                for sample in samples:
                    print_probe(sample, access_jwt)

        if idx + 1 < args.repeat:
            time.sleep(args.sleep_seconds)


if __name__ == "__main__":
    main()