import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote
from urllib.request import Request, urlopen

from app.integrations.ozone.auth import get_test_viewer_access_jwt

LABELER_DID = "did:plc:rh3vjqs4npfpmnkkmx4u4bzj"
APPVIEW_BASE_URL = "https://public.api.bsky.app"


def fetch_post_thread(uri: str) -> tuple[dict, dict]:
    url = (
        f"{APPVIEW_BASE_URL}/xrpc/app.bsky.feed.getPostThread"
        f"?uri={quote(uri, safe='')}&depth=0"
    )

    headers = {
        "Accept": "application/json",
        "atproto-accept-labelers": LABELER_DID,
    }

    access_jwt = get_test_viewer_access_jwt()
    if access_jwt:
        headers["Authorization"] = f"Bearer {access_jwt}"

    req = Request(url, headers=headers, method="GET")
    with urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read().decode("utf-8"))
        resp_headers = dict(resp.headers.items())
    return data, resp_headers


def extract_visibility_info(data: dict, headers: dict) -> dict:
    thread = data.get("thread", {})
    post = thread.get("post", {}) if isinstance(thread, dict) else {}

    post_labels = post.get("labels", []) or []
    author_labels = (post.get("author") or {}).get("labels", []) or []
    content_labelers = headers.get("atproto-content-labelers", "") or ""

    visible = any(lbl.get("src") == LABELER_DID for lbl in post_labels) or any(
        lbl.get("src") == LABELER_DID for lbl in author_labels
    )

    return {
        "post_uri": post.get("uri"),
        "content_labelers": content_labelers,
        "post_labels": post_labels,
        "author_labels": author_labels,
        "visible": visible,
    }


def load_uris(path: Path) -> list[str]:
    lines = path.read_text(encoding="utf-8").splitlines()
    uris = [line.strip() for line in lines if line.strip() and not line.strip().startswith("#")]
    if not uris:
        raise RuntimeError(f"No URIs found in {path}")
    return uris


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def main() -> None:
    if len(sys.argv) < 2:
        print(
            "Usage: python scripts/monitor_label_visibility.py <uris.txt> [interval_seconds] [iterations]"
        )
        raise SystemExit(1)

    uri_file = Path(sys.argv[1])
    interval_seconds = int(sys.argv[2]) if len(sys.argv) >= 3 else 300
    iterations = int(sys.argv[3]) if len(sys.argv) >= 4 else 0  # 0 = forever

    uris = load_uris(uri_file)
    state_file = uri_file.with_suffix(".state.json")
    log_file = uri_file.with_suffix(".monitor.log")

    if state_file.exists():
        previous_state = json.loads(state_file.read_text(encoding="utf-8"))
    else:
        previous_state = {}

    cycle = 0
    while True:
        cycle += 1
        print(f"\n=== Check cycle {cycle} at {now_iso()} ===")

        current_state = {}

        for uri in uris:
            try:
                data, headers = fetch_post_thread(uri)
                info = extract_visibility_info(data, headers)

                current_state[uri] = {
                    "visible": info["visible"],
                    "content_labelers": info["content_labelers"],
                    "post_labels": info["post_labels"],
                    "author_labels": info["author_labels"],
                    "checked_at": now_iso(),
                }

                print("-" * 80)
                print(f"URI: {uri}")
                print(f"Visible: {info['visible']}")
                print(f"atproto-content-labelers: {info['content_labelers']}")
                print(f"Post labels count: {len(info['post_labels'])}")
                print(f"Author labels count: {len(info['author_labels'])}")

                was_visible = previous_state.get(uri, {}).get("visible", False)
                if not was_visible and info["visible"]:
                    message = (
                        f"[{now_iso()}] LABEL BECAME VISIBLE: {uri}\n"
                        f"  atproto-content-labelers: {info['content_labelers']}\n"
                        f"  post_labels: {json.dumps(info['post_labels'])}\n"
                        f"  author_labels: {json.dumps(info['author_labels'])}\n"
                    )
                    print(">>> LABEL BECAME VISIBLE <<<")
                    print(message)
                    with log_file.open("a", encoding="utf-8") as f:
                        f.write(message + "\n")

            except Exception as exc:
                current_state[uri] = {
                    "visible": False,
                    "error": str(exc),
                    "checked_at": now_iso(),
                }
                print("-" * 80)
                print(f"URI: {uri}")
                print(f"ERROR: {exc}")

        state_file.write_text(json.dumps(current_state, indent=2), encoding="utf-8")
        previous_state = current_state

        if iterations and cycle >= iterations:
            break

        time.sleep(interval_seconds)


if __name__ == "__main__":
    main()