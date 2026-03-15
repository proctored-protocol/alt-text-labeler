import sys
from datetime import datetime, timezone

from atproto import (
    FirehoseSubscribeLabelsClient,
    models,
    parse_subscribe_labels_message,
)

LABELER_WSS = "wss://95.216.192.17.sslip.io/xrpc"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def main() -> None:
    target_uri = sys.argv[1] if len(sys.argv) >= 2 else None
    target_val = sys.argv[2] if len(sys.argv) >= 3 else None

    print(f"[{now_iso()}] Connecting to {LABELER_WSS}")
    if target_uri:
        print(f"[{now_iso()}] Target URI: {target_uri}")
    if target_val:
        print(f"[{now_iso()}] Target label value: {target_val}")

    client = FirehoseSubscribeLabelsClient(base_uri=LABELER_WSS)

    def on_message(message) -> None:
        parsed = parse_subscribe_labels_message(message)

        if isinstance(parsed, models.ComAtprotoLabelSubscribeLabels.Labels):
            for lbl in parsed.labels:
                print("-" * 80)
                print(f"[{now_iso()}] LABEL EVENT")
                print(f"  src: {getattr(lbl, 'src', None)}")
                print(f"  uri: {getattr(lbl, 'uri', None)}")
                print(f"  cid: {getattr(lbl, 'cid', None)}")
                print(f"  val: {getattr(lbl, 'val', None)}")
                print(f"  cts: {getattr(lbl, 'cts', None)}")
                print(f"  neg: {getattr(lbl, 'neg', None)}")

                uri_ok = target_uri is None or getattr(lbl, "uri", None) == target_uri
                val_ok = target_val is None or getattr(lbl, "val", None) == target_val

                if uri_ok and val_ok:
                    print(f"[{now_iso()}] >>> MATCH FOUND, stopping stream <<<")
                    client.stop()
                    return

        elif isinstance(parsed, models.ComAtprotoLabelSubscribeLabels.Info):
            print("-" * 80)
            print(f"[{now_iso()}] INFO EVENT")
            print(parsed)

    def on_error(exc: BaseException) -> None:
        print(f"[{now_iso()}] STREAM ERROR: {exc!r}")

    client.start(on_message, on_error)


if __name__ == "__main__":
    main()