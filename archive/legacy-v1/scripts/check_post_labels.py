import json
import sys
from urllib.parse import quote
from urllib.request import Request, urlopen

from app.config import get_settings
from app.integrations.ozone.auth import get_test_viewer_access_jwt

LABELER_DID = "did:plc:rh3vjqs4npfpmnkkmx4u4bzj"


def fetch_post_thread_via_pds(uri: str) -> tuple[dict, dict]:
    settings = get_settings()

    access_jwt = get_test_viewer_access_jwt()
    if not access_jwt:
        raise RuntimeError(
            "TEST_VIEWER_HANDLE and TEST_VIEWER_APP_PASSWORD must be set in .env"
        )

    base_url = settings.bsky_pds_url.rstrip("/")
    url = f"{base_url}/xrpc/app.bsky.feed.getPostThread?uri={quote(uri, safe='')}&depth=0"

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


def main() -> None:
    if len(sys.argv) != 2:
        print("Usage: python scripts/check_post_labels.py <at://post-uri>")
        raise SystemExit(1)

    uri = sys.argv[1]
    data, resp_headers = fetch_post_thread_via_pds(uri)

    print("=== Response header: atproto-content-labelers ===")
    print(resp_headers.get("atproto-content-labelers"))
    print()

    print("=== Raw response top-level keys ===")
    print(sorted(data.keys()))
    print()

    thread = data.get("thread", {})
    post = thread.get("post", {}) if isinstance(thread, dict) else {}

    print("=== Post URI ===")
    print(post.get("uri"))
    print()

    print("=== Post labels ===")
    print(json.dumps(post.get("labels", []), indent=2))
    print()

    author = post.get("author", {})
    print("=== Author labels ===")
    print(json.dumps(author.get("labels", []), indent=2))
    print()

    viewer = post.get("viewer", {})
    print("=== Viewer block ===")
    print(json.dumps(viewer, indent=2))
    print()


if __name__ == "__main__":
    main()