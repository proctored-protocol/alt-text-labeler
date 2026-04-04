import json
import sys
import urllib.parse
import urllib.request
from urllib.error import HTTPError

from app.config import get_settings
from app.integrations.ozone.auth import get_test_viewer_access_jwt

LABELER_DID = "did:plc:rh3vjqs4npfpmnkkmx4u4bzj"
LABELER_ENDPOINT = "https://95.216.192.17.sslip.io"


def fetch_json(url: str, headers: dict | None = None) -> tuple[int, dict | str]:
    req = urllib.request.Request(url, headers=headers or {}, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = resp.read().decode("utf-8")
            try:
                return resp.status, json.loads(body)
            except json.JSONDecodeError:
                return resp.status, body
    except HTTPError as e:
        body = e.read().decode("utf-8")
        try:
            return e.code, json.loads(body)
        except json.JSONDecodeError:
            return e.code, body


def query_labels(base_url: str, post_uri: str, auth_jwt: str | None = None) -> tuple[int, dict | str]:
    params = urllib.parse.urlencode([
        ("uriPatterns", post_uri),
        ("sources", LABELER_DID),
        ("limit", "50"),
    ])
    url = f"{base_url.rstrip('/')}/xrpc/com.atproto.label.queryLabels?{params}"

    headers = {"Accept": "application/json"}
    if auth_jwt:
        headers["Authorization"] = f"Bearer {auth_jwt}"

    return fetch_json(url, headers)


def main() -> None:
    if len(sys.argv) != 2:
        print("Usage: python scripts/check_appview_label_index.py <at://post-uri>")
        raise SystemExit(1)

    post_uri = sys.argv[1]
    settings = get_settings()
    test_viewer_jwt = get_test_viewer_access_jwt()

    targets = [
        ("labeler_direct", LABELER_ENDPOINT, None),
        ("subscriber_pds", settings.bsky_pds_url, test_viewer_jwt),
        ("public_appview", "https://public.api.bsky.app", None),
    ]

    for name, base_url, jwt in targets:
        print()
        print("=" * 80)
        print(name)
        print("=" * 80)
        status, data = query_labels(base_url, post_uri, jwt)
        print("base_url:", base_url)
        print("status:", status)
        if isinstance(data, dict):
            labels = data.get("labels", [])
            print("cursor:", data.get("cursor"))
            print("label_count:", len(labels))
            print(json.dumps(data, indent=2))
        else:
            print(data)


if __name__ == "__main__":
    main()