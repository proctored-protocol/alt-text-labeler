from __future__ import annotations

import argparse
import base64
import json
import os
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

from app.firehose.live_metrics_store import LiveMetricsStore, METRIC_COLUMNS


WINDOWS = [
    ("1m", 60),
    ("10m", 600),
    ("1h", 3600),
    ("24h", 86400),
    ("1w", 604800),
]


def iso_utc(epoch: int | None) -> str | None:
    if epoch is None:
        return None
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(epoch))


def build_summary_payload(store: LiveMetricsStore) -> dict[str, Any]:
    state = store.get_state()
    latest = store.get_latest_second()
    total = store.get_total_summary()

    windows: dict[str, Any] = {}
    for name, seconds in WINDOWS:
        row = store.get_window_summary(seconds)
        windows[name] = row

    return {
        "generated_at_utc": iso_utc(int(time.time())),
        "state": {
            **state,
            "started_at_iso": iso_utc(state.get("started_at")),
            "updated_at_iso": iso_utc(state.get("updated_at")),
            "last_message_at_iso": iso_utc(state.get("last_message_at")),
        },
        "latest_second": {
            **latest,
            "ts_iso": iso_utc(latest.get("ts_epoch")),
        },
        "windows": windows,
        "total": {
            **total,
            "started_at_iso": iso_utc(total.get("started_at")),
        },
    }


def build_series_payload(store: LiveMetricsStore, *, range_seconds: int) -> dict[str, Any]:
    data = store.get_series(range_seconds=range_seconds, max_points=720)
    points = data["points"]

    transformed = []
    for point in points:
        transformed.append(
            {
                "ts_epoch": point["bucket_epoch"],
                "ts_iso": iso_utc(point["bucket_epoch"]),
                "bucket_seconds": point["bucket_seconds"],
                **{col: point[col] for col in METRIC_COLUMNS},
                **{f"{col}_rate": point[f"{col}_rate"] for col in METRIC_COLUMNS},
            }
        )

    return {
        "generated_at_utc": iso_utc(int(time.time())),
        "range_seconds": data["range_seconds"],
        "bucket_seconds": data["bucket_seconds"],
        "points": transformed,
    }


def html_page() -> str:
    return """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Live Firehose Dashboard</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
  body {
    font-family: system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif;
    margin: 0;
    padding: 20px;
    background: #0b1020;
    color: #ecf1ff;
  }
  h1 {
    margin: 0 0 8px 0;
    font-size: 26px;
  }
  .sub {
    color: #aab7d6;
    margin-bottom: 18px;
  }
  .toolbar {
    display: flex;
    gap: 8px;
    flex-wrap: wrap;
    margin-bottom: 16px;
  }
  button {
    background: #19233f;
    color: #e9f0ff;
    border: 1px solid #32436f;
    border-radius: 10px;
    padding: 8px 12px;
    cursor: pointer;
  }
  button.active {
    background: #2f6feb;
    border-color: #2f6feb;
  }
  .grid {
    display: grid;
    grid-template-columns: repeat(4, minmax(0, 1fr));
    gap: 12px;
    margin-bottom: 18px;
  }
  .card, .chart-card, .table-card {
    background: #111933;
    border: 1px solid #25345f;
    border-radius: 14px;
    box-shadow: 0 1px 2px rgba(0,0,0,0.25);
  }
  .card {
    padding: 14px 16px;
  }
  .card h3 {
    margin: 0 0 6px 0;
    font-size: 13px;
    color: #9cb0db;
    font-weight: 600;
  }
  .big {
    font-size: 28px;
    font-weight: 700;
  }
  .small {
    color: #90a2cb;
    font-size: 12px;
    margin-top: 4px;
  }
  .chart-card, .table-card {
    padding: 14px 16px 16px 16px;
    margin-bottom: 16px;
  }
  .chart-title {
    margin: 0 0 4px 0;
    font-size: 16px;
    font-weight: 700;
  }
  .chart-sub {
    margin: 0 0 10px 0;
    color: #97a8d0;
    font-size: 13px;
  }
  .legend {
    display: flex;
    flex-wrap: wrap;
    gap: 12px;
    margin-bottom: 10px;
    font-size: 12px;
    color: #c9d7fb;
  }
  .legend-item {
    display: inline-flex;
    align-items: center;
    gap: 6px;
  }
  .swatch {
    width: 10px;
    height: 10px;
    border-radius: 999px;
    display: inline-block;
  }
  canvas {
    width: 100%;
    height: 260px;
    display: block;
  }
  table {
    width: 100%;
    border-collapse: collapse;
    font-size: 13px;
  }
  th, td {
    text-align: right;
    padding: 8px 10px;
    border-bottom: 1px solid #233257;
  }
  th:first-child, td:first-child {
    text-align: left;
  }
  th {
    color: #b8c8ef;
  }
  td {
    color: #eef4ff;
  }
  .mono {
    font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
  }
  .footer {
    color: #93a6d1;
    font-size: 12px;
    margin-top: 10px;
  }
</style>
</head>
<body>
  <h1>Live Firehose Dashboard</h1>
  <div class="sub">Standalone firehose observability. Refreshes every second. Missing/partial alt counts are lightweight live classifications from image alt fields.</div>

  <div class="toolbar">
    <button data-range="60">1m</button>
    <button data-range="600" class="active">10m</button>
    <button data-range="3600">1h</button>
    <button data-range="86400">24h</button>
    <button data-range="604800">1w</button>
  </div>

  <div class="grid">
    <div class="card">
      <h3>Latest posts/sec</h3>
      <div class="big" id="latest-posts">–</div>
      <div class="small" id="latest-posts-ts"></div>
    </div>
    <div class="card">
      <h3>Latest image posts/sec</h3>
      <div class="big" id="latest-images">–</div>
      <div class="small">last second bucket</div>
    </div>
    <div class="card">
      <h3>Latest missing-alt/sec</h3>
      <div class="big" id="latest-missing">–</div>
      <div class="small">live lightweight classification</div>
    </div>
    <div class="card">
      <h3>Collector health</h3>
      <div class="big" id="collector-status">–</div>
      <div class="small mono" id="collector-meta"></div>
    </div>
  </div>

  <div class="chart-card">
    <div class="chart-title">Post volume</div>
    <div class="chart-sub">Posts/sec and image posts/sec</div>
    <div class="legend" id="legend-volume"></div>
    <canvas id="chart-volume" width="1200" height="260"></canvas>
  </div>

  <div class="chart-card">
    <div class="chart-title">Accessibility signal</div>
    <div class="chart-sub">Missing-alt/sec and partial-alt/sec</div>
    <div class="legend" id="legend-access"></div>
    <canvas id="chart-access" width="1200" height="260"></canvas>
  </div>

  <div class="chart-card">
    <div class="chart-title">Other media</div>
    <div class="chart-sub">GIF/sec and video/sec (best-effort based on parser-exposed embed info)</div>
    <div class="legend" id="legend-media"></div>
    <canvas id="chart-media" width="1200" height="260"></canvas>
  </div>

  <div class="table-card">
    <div class="chart-title">Rolling windows</div>
    <div class="chart-sub">Totals and average per second</div>
    <table id="window-table">
      <thead>
        <tr>
          <th>Window</th>
          <th>Posts</th>
          <th>Images</th>
          <th>Missing alt</th>
          <th>Partial alt</th>
          <th>GIF</th>
          <th>Video</th>
          <th>Avg posts/sec</th>
          <th>Avg missing/sec</th>
        </tr>
      </thead>
      <tbody></tbody>
    </table>
  </div>

  <div class="table-card">
    <div class="chart-title">Totals since start</div>
    <div class="chart-sub">Collector lifetime totals</div>
    <table id="total-table">
      <tbody></tbody>
    </table>
  </div>

  <div class="footer mono" id="footer"></div>

<script>
let currentRange = 600;

const SERIES = {
  volume: [
    { key: "post_create_count_rate", label: "posts/sec", color: "#5aa9ff" },
    { key: "image_post_count_rate", label: "image posts/sec", color: "#ffd166" },
  ],
  access: [
    { key: "missing_alt_post_count_rate", label: "missing-alt/sec", color: "#ff5d73" },
    { key: "partial_alt_post_count_rate", label: "partial-alt/sec", color: "#06d6a0" },
  ],
  media: [
    { key: "gif_post_count_rate", label: "gif/sec", color: "#c77dff" },
    { key: "video_post_count_rate", label: "video/sec", color: "#7ae582" },
  ],
};

function fmt(n) {
  if (n === null || n === undefined) return "–";
  return Number(n).toLocaleString();
}

function fmtRate(n) {
  if (n === null || n === undefined) return "–";
  return Number(n).toFixed(2);
}

function q(id) {
  return document.getElementById(id);
}

function setLegend(id, series) {
  const el = q(id);
  el.innerHTML = "";
  for (const s of series) {
    const item = document.createElement("div");
    item.className = "legend-item";
    item.innerHTML = `<span class="swatch" style="background:${s.color}"></span><span>${s.label}</span>`;
    el.appendChild(item);
  }
}

function drawChart(canvasId, points, series) {
  const canvas = q(canvasId);
  const ctx = canvas.getContext("2d");
  const w = canvas.width;
  const h = canvas.height;
  const pad = { top: 14, right: 18, bottom: 26, left: 48 };

  ctx.clearRect(0, 0, w, h);
  ctx.fillStyle = "#111933";
  ctx.fillRect(0, 0, w, h);

  const W = w - pad.left - pad.right;
  const H = h - pad.top - pad.bottom;

  const values = [];
  for (const s of series) {
    for (const p of points) {
      const v = Number(p[s.key] || 0);
      values.push(v);
    }
  }

  const yMax = Math.max(1, ...values);

  function x(i) {
    if (points.length <= 1) return pad.left + W / 2;
    return pad.left + (i * W) / (points.length - 1);
  }

  function y(v) {
    return pad.top + H - (Number(v) / yMax) * H;
  }

  ctx.strokeStyle = "#233257";
  ctx.lineWidth = 1;
  for (let i = 0; i <= 4; i++) {
    const yy = pad.top + (H * i) / 4;
    ctx.beginPath();
    ctx.moveTo(pad.left, yy);
    ctx.lineTo(pad.left + W, yy);
    ctx.stroke();

    ctx.fillStyle = "#93a6d1";
    ctx.font = "11px system-ui";
    ctx.textAlign = "right";
    ctx.fillText(String((yMax - (yMax * i) / 4).toFixed(1)), pad.left - 6, yy + 4);
  }

  ctx.strokeStyle = "#4b5f8d";
  ctx.beginPath();
  ctx.moveTo(pad.left, pad.top + H);
  ctx.lineTo(pad.left + W, pad.top + H);
  ctx.stroke();

  ctx.beginPath();
  ctx.moveTo(pad.left, pad.top);
  ctx.lineTo(pad.left, pad.top + H);
  ctx.stroke();

  const tickIdx = [...new Set([0, Math.floor((points.length - 1) / 2), points.length - 1])];
  ctx.fillStyle = "#93a6d1";
  ctx.textAlign = "center";
  for (const idx of tickIdx) {
    if (idx < 0 || idx >= points.length) continue;
    const xx = x(idx);
    const iso = points[idx].ts_iso || "";
    const label = iso.slice(11, 16);
    ctx.fillText(label, xx, pad.top + H + 18);
  }

  for (const s of series) {
    ctx.strokeStyle = s.color;
    ctx.lineWidth = 2.2;
    ctx.beginPath();
    points.forEach((p, i) => {
      const xx = x(i);
      const yy = y(p[s.key] || 0);
      if (i === 0) ctx.moveTo(xx, yy);
      else ctx.lineTo(xx, yy);
    });
    ctx.stroke();
  }
}

async function fetchSummary() {
  const resp = await fetch("/api/summary");
  return await resp.json();
}

async function fetchSeries(rangeSeconds) {
  const resp = await fetch(`/api/series?range_seconds=${rangeSeconds}`);
  return await resp.json();
}

function renderSummary(summary) {
  const latest = summary.latest_second || {};
  const state = summary.state || {};
  const windows = summary.windows || {};
  const total = summary.total || {};

  q("latest-posts").textContent = fmt(latest.post_create_count);
  q("latest-posts-ts").textContent = latest.ts_iso || "–";
  q("latest-images").textContent = fmt(latest.image_post_count);
  q("latest-missing").textContent = fmt(latest.missing_alt_post_count);
  q("collector-status").textContent = state.current_status || "–";
  q("collector-meta").textContent =
    `seq=${state.last_seq ?? "–"} reconnects=${state.reconnect_count ?? 0} errors=${state.error_count ?? 0}`;

  const body = q("window-table").querySelector("tbody");
  body.innerHTML = "";

  for (const [name, row] of Object.entries(windows)) {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${name}</td>
      <td>${fmt(row.post_create_count)}</td>
      <td>${fmt(row.image_post_count)}</td>
      <td>${fmt(row.missing_alt_post_count)}</td>
      <td>${fmt(row.partial_alt_post_count)}</td>
      <td>${fmt(row.gif_post_count)}</td>
      <td>${fmt(row.video_post_count)}</td>
      <td>${fmtRate(row.post_create_count_avg_per_sec)}</td>
      <td>${fmtRate(row.missing_alt_post_count_avg_per_sec)}</td>
    `;
    body.appendChild(tr);
  }

  const totalBody = q("total-table").querySelector("tbody");
  totalBody.innerHTML = `
    <tr><th>Started at</th><td>${total.started_at_iso || "–"}</td></tr>
    <tr><th>Uptime seconds</th><td>${fmt(total.uptime_seconds)}</td></tr>
    <tr><th>Total posts</th><td>${fmt(total.post_create_count)}</td></tr>
    <tr><th>Total image posts</th><td>${fmt(total.image_post_count)}</td></tr>
    <tr><th>Total missing-alt posts</th><td>${fmt(total.missing_alt_post_count)}</td></tr>
    <tr><th>Total partial-alt posts</th><td>${fmt(total.partial_alt_post_count)}</td></tr>
    <tr><th>Total GIF posts</th><td>${fmt(total.gif_post_count)}</td></tr>
    <tr><th>Total video posts</th><td>${fmt(total.video_post_count)}</td></tr>
    <tr><th>Avg posts/sec since start</th><td>${fmtRate(total.post_create_count_avg_per_sec)}</td></tr>
    <tr><th>Avg missing-alt/sec since start</th><td>${fmtRate(total.missing_alt_post_count_avg_per_sec)}</td></tr>
  `;

  q("footer").textContent =
    `updated=${summary.generated_at_utc || "–"} last_message=${state.last_message_at_iso || "–"} seq=${state.last_seq ?? "–"}`;
}

function setActiveRangeButton() {
  document.querySelectorAll("button[data-range]").forEach((btn) => {
    if (Number(btn.dataset.range) === currentRange) btn.classList.add("active");
    else btn.classList.remove("active");
  });
}

async function refreshAll() {
  try {
    const [summary, seriesPayload] = await Promise.all([
      fetchSummary(),
      fetchSeries(currentRange),
    ]);

    renderSummary(summary);

    const points = seriesPayload.points || [];
    drawChart("chart-volume", points, SERIES.volume);
    drawChart("chart-access", points, SERIES.access);
    drawChart("chart-media", points, SERIES.media);
  } catch (err) {
    console.error(err);
  }
}

setLegend("legend-volume", SERIES.volume);
setLegend("legend-access", SERIES.access);
setLegend("legend-media", SERIES.media);
setActiveRangeButton();

document.querySelectorAll("button[data-range]").forEach((btn) => {
  btn.addEventListener("click", async () => {
    currentRange = Number(btn.dataset.range);
    setActiveRangeButton();
    await refreshAll();
  });
});

refreshAll();
setInterval(refreshAll, 1000);
</script>
</body>
</html>
"""


class DashboardHandler(BaseHTTPRequestHandler):
    store: LiveMetricsStore
    dashboard_user: str | None
    dashboard_password: str | None

    def _auth_required(self) -> bool:
        return bool(self.dashboard_user and self.dashboard_password)

    def _authorized(self) -> bool:
        if not self._auth_required():
            return True

        header = self.headers.get("Authorization", "")
        if not header.startswith("Basic "):
            return False

        try:
            decoded = base64.b64decode(header.split(" ", 1)[1]).decode("utf-8")
        except Exception:
            return False

        if ":" not in decoded:
            return False

        username, password = decoded.split(":", 1)
        return username == self.dashboard_user and password == self.dashboard_password

    def _send_auth_challenge(self) -> None:
        self.send_response(401)
        self.send_header("WWW-Authenticate", 'Basic realm="Live Firehose Dashboard"')
        self.end_headers()

    def _send_json(self, payload: dict[str, Any], status: int = 200) -> None:
        raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(raw)

    def _send_html(self, html: str, status: int = 200) -> None:
        raw = html.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self) -> None:
        if not self._authorized():
            self._send_auth_challenge()
            return

        parsed = urlparse(self.path)

        if parsed.path == "/":
            self._send_html(html_page())
            return

        if parsed.path == "/api/summary":
            self._send_json(build_summary_payload(self.store))
            return

        if parsed.path == "/api/series":
            qs = parse_qs(parsed.query)
            try:
                range_seconds = int((qs.get("range_seconds") or ["600"])[0])
            except Exception:
                range_seconds = 600

            range_seconds = max(60, min(range_seconds, 604800))
            self._send_json(build_series_payload(self.store, range_seconds=range_seconds))
            return

        self._send_json({"error": "not_found"}, status=404)


def main() -> None:
    parser = argparse.ArgumentParser(description="Serve the live firehose dashboard.")
    parser.add_argument(
        "--db-path",
        default="data/firehose_live.sqlite3",
        help="SQLite database file written by the collector",
    )
    parser.add_argument(
        "--host",
        default="0.0.0.0",
        help="Bind host",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8765,
        help="Bind port",
    )
    args = parser.parse_args()

    store = LiveMetricsStore(args.db_path)

    handler_cls = type(
        "BoundDashboardHandler",
        (DashboardHandler,),
        {
            "store": store,
            "dashboard_user": os.getenv("FIREHOSE_DASHBOARD_USERNAME"),
            "dashboard_password": os.getenv("FIREHOSE_DASHBOARD_PASSWORD"),
        },
    )

    server = ThreadingHTTPServer((args.host, args.port), handler_cls)
    print(
        json.dumps(
            {
                "event": "firehose_live_dashboard_started",
                "host": args.host,
                "port": args.port,
                "db_path": args.db_path,
                "auth_enabled": bool(
                    os.getenv("FIREHOSE_DASHBOARD_USERNAME")
                    and os.getenv("FIREHOSE_DASHBOARD_PASSWORD")
                ),
            }
        ),
        flush=True,
    )
    server.serve_forever()


if __name__ == "__main__":
    main()