from __future__ import annotations

import argparse
import json
from typing import Any
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen

from sqlalchemy import inspect, text

from app.db import engine


PUBLIC_API_BASE_URL = "https://public.api.bsky.app"


def http_json(url: str, *, timeout: int = 30) -> dict[str, Any]:
    req = Request(url, headers={"Accept": "application/json"}, method="GET")
    with urlopen(req, timeout=timeout) as resp:
        raw = resp.read().decode("utf-8")
        return json.loads(raw) if raw else {}


def parse_post_url(post_url: str) -> tuple[str, str]:
    parsed = urlparse(post_url)
    if parsed.scheme != "https" or parsed.netloc != "bsky.app":
        raise ValueError("Post URL must be an https://bsky.app/profile/.../post/... link")

    parts = [p for p in parsed.path.split("/") if p]
    if len(parts) != 4 or parts[0] != "profile" or parts[2] != "post":
        raise ValueError("Post URL must look like https://bsky.app/profile/<handle-or-did>/post/<rkey>")

    return parts[1], parts[3]


def resolve_profile_token_to_did(profile_token: str, *, timeout: int) -> str:
    if profile_token.startswith("did:"):
        return profile_token

    params = urlencode({"handle": profile_token})
    url = f"{PUBLIC_API_BASE_URL}/xrpc/com.atproto.identity.resolveHandle?{params}"
    payload = http_json(url, timeout=timeout)
    did = payload.get("did")
    if not did:
        raise RuntimeError(f"resolveHandle did not return a DID for {profile_token}")
    return did


def fetch_post_cid(at_uri: str, *, timeout: int) -> str | None:
    params = urlencode([("uris", at_uri)])
    url = f"{PUBLIC_API_BASE_URL}/xrpc/app.bsky.feed.getPosts?{params}"
    payload = http_json(url, timeout=timeout)
    posts = payload.get("posts") or []
    if not posts:
        return None
    return posts[0].get("cid")


def resolve_post(post_url: str, *, timeout: int) -> tuple[str, str | None]:
    profile_token, rkey = parse_post_url(post_url)
    did = resolve_profile_token_to_did(profile_token, timeout=timeout)
    at_uri = f"at://{did}/app.bsky.feed.post/{rkey}"
    cid = fetch_post_cid(at_uri, timeout=timeout)
    return at_uri, cid


def choose_order_by(columns: list[str]) -> str | None:
    preferred = [
        "published_at",
        "updated_at",
        "created_at",
        "record_created_at",
        "checked_at",
        "last_forced_checked_at",
        "last_subscriber_checked_at",
        "id",
        "cid",
    ]
    for col in preferred:
        if col in columns:
            return col
    return None


def fetch_table_rows(
    *,
    conn,
    table_name: str,
    columns: list[str],
    where_sql: str,
    params: dict[str, Any],
    limit: int = 100,
) -> list[dict[str, Any]]:
    order_by = choose_order_by(columns)

    sql = f"SELECT * FROM {table_name} WHERE {where_sql}"
    if order_by is not None:
        sql += f" ORDER BY {order_by} DESC"
    sql += " LIMIT :limit"

    query_params = dict(params)
    query_params["limit"] = limit

    result = conn.execute(text(sql), query_params).mappings()
    return [dict(r) for r in result]


def fetch_rows_for_uri(uri: str) -> dict[str, Any]:
    inspector = inspect(engine)
    tables = inspector.get_table_names()

    output: dict[str, Any] = {
        "uri": uri,
        "tables": {},
    }

    publication_ids: list[int] = []

    with engine.connect() as conn:
        if "label_publication" in tables:
            cols = [c["name"] for c in inspector.get_columns("label_publication")]
            rows: list[dict[str, Any]] = []
            if "uri" in cols:
                rows = fetch_table_rows(
                    conn=conn,
                    table_name="label_publication",
                    columns=cols,
                    where_sql="uri = :uri",
                    params={"uri": uri},
                )
                if "id" in cols:
                    publication_ids = [r["id"] for r in rows if isinstance(r.get("id"), int)]
            output["tables"]["label_publication"] = {
                "columns": cols,
                "rows": rows,
            }

        if "post_evaluation" in tables:
            cols = [c["name"] for c in inspector.get_columns("post_evaluation")]
            rows: list[dict[str, Any]] = []
            if "uri" in cols:
                rows = fetch_table_rows(
                    conn=conn,
                    table_name="post_evaluation",
                    columns=cols,
                    where_sql="uri = :uri",
                    params={"uri": uri},
                )
            output["tables"]["post_evaluation"] = {
                "columns": cols,
                "rows": rows,
            }

        if "label_visibility" in tables:
            cols = [c["name"] for c in inspector.get_columns("label_visibility")]
            rows: list[dict[str, Any]] = []
            if "uri" in cols:
                rows = fetch_table_rows(
                    conn=conn,
                    table_name="label_visibility",
                    columns=cols,
                    where_sql="uri = :uri",
                    params={"uri": uri},
                )
            output["tables"]["label_visibility"] = {
                "columns": cols,
                "rows": rows,
            }

        if "publish_job" in tables:
            cols = [c["name"] for c in inspector.get_columns("publish_job")]
            rows: list[dict[str, Any]] = []
            if "publication_id" in cols and publication_ids:
                sql = "SELECT * FROM publish_job WHERE publication_id = ANY(:publication_ids)"
                order_by = choose_order_by(cols)
                if order_by is not None:
                    sql += f" ORDER BY {order_by} DESC"
                sql += " LIMIT :limit"

                result = conn.execute(
                    text(sql),
                    {"publication_ids": publication_ids, "limit": 100},
                ).mappings()
                rows = [dict(r) for r in result]

            output["tables"]["publish_job"] = {
                "columns": cols,
                "rows": rows,
            }

    return output


def main() -> None:
    parser = argparse.ArgumentParser(description="Inspect server-side workflow records for one Bluesky post.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--post-url", help="Bluesky HTTPS post URL")
    group.add_argument("--at-uri", help="AT URI of the post")
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args()

    if args.post_url:
        at_uri, cid = resolve_post(args.post_url, timeout=args.timeout)
    else:
        at_uri = args.at_uri
        cid = fetch_post_cid(at_uri, timeout=args.timeout)

    result = {
        "resolved": {
            "at_uri": at_uri,
            "cid": cid,
        },
        "workflow": fetch_rows_for_uri(at_uri),
    }

    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()