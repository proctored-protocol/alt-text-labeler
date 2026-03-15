import json
import sys
from urllib.parse import quote
from urllib.request import Request, urlopen

from app.config import get_settings
from app.integrations.ozone.auth import get_test_viewer_access_jwt

LABELER_DID = "did:plc:rh3vjqs4npfpmnkkmx4u4bzj"


def fetch_json(url: str) -> tuple[dict, dict]:
    settings = get_settings()
    access_jwt = get_test_viewer_access_jwt()
    if not access_jwt:
        raise RuntimeError(
            "TEST_VIEWER_HANDLE and TEST_VIEWER_APP_PASSWORD must be set in .env"
        )

    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {access_jwt}",
        "atproto-accept-labelers": LABELER_DID,
    }

    req = Request(url, headers=headers, method="GET")
    with urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read().decode("utf-8"))
        resp_headers = dict(resp.headers.items())
    return data, resp_headers


def print_section(title: str) -> None:
    print()
    print("=" * 80)
    print(title)
    print("=" * 80)


def main() -> None:
    if len(sys.argv) < 2:
        print(
            "Usage: python scripts/check_hydration_endpoints.py <at://post-uri> [actor-did-or-handle]"
        )
        raise SystemExit(1)

    settings = get_settings()
    base_url = settings.bsky_pds_url.rstrip("/")

    post_uri = sys.argv[1]
    actor = sys.argv[2] if len(sys.argv) >= 3 else None

    # 1) getPostThread
    thread_url = (
        f"{base_url}/xrpc/app.bsky.feed.getPostThread"
        f"?uri={quote(post_uri, safe='')}&depth=0"
    )
    thread_data, thread_headers = fetch_json(thread_url)

    print_section("app.bsky.feed.getPostThread")
    print("atproto-content-labelers:", thread_headers.get("atproto-content-labelers"))
    thread = thread_data.get("thread", {})
    post = thread.get("post", {}) if isinstance(thread, dict) else {}
    print("post.uri:", post.get("uri"))
    print("post.cid:", post.get("cid"))
    print("post.labels:", json.dumps(post.get("labels", []), indent=2))
    print("author.labels:", json.dumps((post.get("author") or {}).get("labels", []), indent=2))

    # 2) getPosts
    posts_url = (
        f"{base_url}/xrpc/app.bsky.feed.getPosts"
        f"?uris={quote(post_uri, safe='')}"
    )
    posts_data, posts_headers = fetch_json(posts_url)

    print_section("app.bsky.feed.getPosts")
    print("atproto-content-labelers:", posts_headers.get("atproto-content-labelers"))
    posts = posts_data.get("posts", []) or []
    if posts:
        p = posts[0]
        print("post.uri:", p.get("uri"))
        print("post.cid:", p.get("cid"))
        print("post.labels:", json.dumps(p.get("labels", []), indent=2))
        print("author.labels:", json.dumps((p.get("author") or {}).get("labels", []), indent=2))
    else:
        print("No posts returned")

    # 3) Optional actor profile check
    if actor:
        profile_url = (
            f"{base_url}/xrpc/app.bsky.actor.getProfile"
            f"?actor={quote(actor, safe='')}"
        )
        profile_data, profile_headers = fetch_json(profile_url)

        print_section("app.bsky.actor.getProfile")
        print("atproto-content-labelers:", profile_headers.get("atproto-content-labelers"))
        print("actor.did:", profile_data.get("did"))
        print("actor.handle:", profile_data.get("handle"))
        print("actor.labels:", json.dumps(profile_data.get("labels", []), indent=2))


if __name__ == "__main__":
    main()