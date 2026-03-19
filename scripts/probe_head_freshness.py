import argparse
import logging
import sys
import time
from collections import deque
from datetime import datetime, timezone

from atproto import (
    FirehoseSubscribeReposClient,
    models,
    parse_subscribe_repos_message,
)

from app.config import get_settings
from app.parsing.posts import iter_post_creates
from app.services.evaluator import evaluate_post

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("probe_head_freshness")


def parse_created_at(value: str | None) -> datetime | None:
    if not value:
        return None
    s = value.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        return None


def pctl(values: list[float], q: float) -> float | None:
    if not values:
        return None
    values = sorted(values)
    idx = (len(values) - 1) * q
    lo = int(idx)
    hi = min(lo + 1, len(values) - 1)
    if lo == hi:
        return values[lo]
    frac = idx - lo
    return values[lo] * (1 - frac) + values[hi] * frac


class FreshnessProbe:
    def __init__(self, base_uri: str, print_each: bool) -> None:
        self.settings = get_settings()
        self.base_uri = base_uri
        self.print_each = print_each

        self.minute_started = time.time()
        self.commit_count = 0
        self.post_create_count = 0
        self.image_post_count = 0
        self.label_candidate_count = 0
        self.missing_count = 0
        self.partial_count = 0

        self.image_ages: deque[float] = deque()
        self.label_ages: deque[float] = deque()

        self.client = FirehoseSubscribeReposClient(
            params=None,  # no saved cursor: fresh connection for probe purposes
            base_uri=self.base_uri,
        )

    def run(self) -> None:
        logger.info("starting probe", extra={"base_uri": self.base_uri})
        self.client.start(self.on_message, self.on_callback_error)

    def on_callback_error(self, exc: BaseException) -> None:
        logger.exception("probe_callback_error", exc_info=exc)

    def _maybe_flush_minute(self) -> None:
        now = time.time()
        if now - self.minute_started < 60:
            return

        img_vals = list(self.image_ages)
        lbl_vals = list(self.label_ages)

        def fmt(values: list[float]) -> str:
            if not values:
                return "n=0"
            return (
                f"n={len(values)} "
                f"p50={pctl(values, 0.50):.1f}s "
                f"p95={pctl(values, 0.95):.1f}s "
                f"max={max(values):.1f}s"
            )

        print("\n=== probe minute summary ===")
        print(f"utc_now:               {datetime.now(timezone.utc).isoformat()}")
        print(f"base_uri:              {self.base_uri}")
        print(f"commit_count:          {self.commit_count}")
        print(f"post_create_count:     {self.post_create_count}")
        print(f"image_post_count:      {self.image_post_count}")
        print(f"label_candidate_count: {self.label_candidate_count}")
        print(f"missing_count:         {self.missing_count}")
        print(f"partial_count:         {self.partial_count}")
        print(f"image_age:             {fmt(img_vals)}")
        print(f"label_candidate_age:   {fmt(lbl_vals)}")
        sys.stdout.flush()

        self.minute_started = now
        self.commit_count = 0
        self.post_create_count = 0
        self.image_post_count = 0
        self.label_candidate_count = 0
        self.missing_count = 0
        self.partial_count = 0
        self.image_ages.clear()
        self.label_ages.clear()

    def on_message(self, message) -> None:
        parsed = parse_subscribe_repos_message(message)
        if not isinstance(parsed, models.ComAtprotoSyncSubscribeRepos.Commit):
            return

        self.commit_count += 1

        for post in iter_post_creates(parsed):
            self.post_create_count += 1

            if not post.image_alts:
                continue

            self.image_post_count += 1

            created_ts = parse_created_at(post.created_at)
            if created_ts is None:
                continue

            age_s = (
                datetime.now(timezone.utc) - created_ts.astimezone(timezone.utc)
            ).total_seconds()
            self.image_ages.append(age_s)

            result = evaluate_post(
                post=post,
                missing_label=self.settings.label_missing_alt,
                partial_label=self.settings.label_partial_alt,
                last_seen_seq=parsed.seq,
            )

            derived_label = None if result is None else result.derived_label
            if derived_label is not None:
                self.label_candidate_count += 1
                self.label_ages.append(age_s)

                if derived_label == self.settings.label_missing_alt:
                    self.missing_count += 1
                elif derived_label == self.settings.label_partial_alt:
                    self.partial_count += 1

            if self.print_each:
                print(
                    f"age_s={age_s:.1f} "
                    f"derived_label={derived_label} "
                    f"created_at={post.created_at} "
                    f"uri={post.uri}"
                )
                sys.stdout.flush()

        self._maybe_flush_minute()


def main() -> None:
    settings = get_settings()

    parser = argparse.ArgumentParser(
        description="Freshness probe with no DB writes / no publishing"
    )
    parser.add_argument(
        "--base-uri",
        default=settings.firehose_base_uri,
        help="WebSocket firehose base URI",
    )
    parser.add_argument(
        "--print-each",
        action="store_true",
        help="Print every image post sample as it arrives",
    )
    args = parser.parse_args()

    FreshnessProbe(
        base_uri=args.base_uri,
        print_each=args.print_each,
    ).run()


if __name__ == "__main__":
    main()