from __future__ import annotations

import argparse
import base64
import json
import os
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
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


def parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


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


def load_recent_jsonl(path: Path, max_points: int) -> list[dict[str, Any]]:
    if not path.exists():
        return []

    items: deque[dict[str, Any]] = deque(maxlen=max_points)
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                items.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return list(items)


def diff_int(current: int | None, previous: int | None) -> int | None:
    if current is None or previous is None:
        return None
    return int(current) - int(previous)


def build_intake_series_payload(intake_metrics_jsonl: str, *, range_seconds: int) -> dict[str, Any]:
    path = Path(intake_metrics_jsonl)
    snapshots = load_recent_jsonl(path, max_points=20000)

    if not snapshots:
        return {
            "generated_at_utc": iso_utc(int(time.time())),
            "range_seconds": range_seconds,
            "points": [],
            "latest": None,
            "status": "unavailable",
            "source_file": str(path),
        }

    snapshots.sort(key=lambda x: x.get("ts", ""))

    latest_ts = parse_ts(snapshots[-1].get("ts"))
    if latest_ts is None:
        return {
            "generated_at_utc": iso_utc(int(time.time())),
            "range_seconds": range_seconds,
            "points": [],
            "latest": None,
            "status": "unavailable",
            "source_file": str(path),
        }

    cutoff = latest_ts - timedelta(seconds=range_seconds)
    filtered = []
    for snap in snapshots:
        ts = parse_ts(snap.get("ts"))
        if ts is None:
            continue
        if ts >= cutoff:
            filtered.append(snap)

    points: list[dict[str, Any]] = []
    for snap in filtered:
        live_latest_minute = snap.get("live_latest_completed_minute") or {}
        live_window_10m = snap.get("live_window_10m") or {}

        point = {
            "ts": snap.get("ts"),
            "live_monitor_last_seq": snap.get("live_monitor_last_seq"),
            "intake_cursor_last_seq": snap.get("intake_cursor_last_seq"),
            "post_eval_max_last_seen_seq": snap.get("post_eval_max_last_seen_seq"),
            "seq_gap_live_minus_intake": snap.get("seq_gap_live_minus_intake"),
            "seq_gap_live_minus_post_eval": snap.get("seq_gap_live_minus_post_eval"),
            "seq_gap_post_eval_minus_cursor": snap.get("seq_gap_post_eval_minus_cursor"),
            "cursor_updated_age_seconds": snap.get("cursor_updated_age_seconds"),
            "post_eval_updated_age_seconds": snap.get("post_eval_updated_age_seconds"),
            "evaluated_rows_2m": snap.get("evaluated_rows_2m"),
            "evaluated_rows_10m": snap.get("evaluated_rows_10m"),
            "labeled_rows_10m": snap.get("labeled_rows_10m"),
            "live_latest_minute_post_create_count": live_latest_minute.get("post_create_count"),
            "live_latest_minute_image_post_count": live_latest_minute.get("image_post_count"),
            "live_latest_minute_missing_alt_post_count": live_latest_minute.get("missing_alt_post_count"),
            "live_window_10m_post_create_count": live_window_10m.get("post_create_count"),
            "live_window_10m_image_post_count": live_window_10m.get("image_post_count"),
            "live_window_10m_missing_alt_post_count": live_window_10m.get("missing_alt_post_count"),
        }
        points.append(point)

    for i, point in enumerate(points):
        prev = points[i - 1] if i > 0 else None
        point["live_seq_delta"] = diff_int(
            point.get("live_monitor_last_seq"),
            prev.get("live_monitor_last_seq") if prev else None,
        )
        point["intake_seq_delta"] = diff_int(
            point.get("intake_cursor_last_seq"),
            prev.get("intake_cursor_last_seq") if prev else None,
        )
        point["post_eval_seq_delta"] = diff_int(
            point.get("post_eval_max_last_seen_seq"),
            prev.get("post_eval_max_last_seen_seq") if prev else None,
        )
        point["gap_delta"] = diff_int(
            point.get("seq_gap_live_minus_intake"),
            prev.get("seq_gap_live_minus_intake") if prev else None,
        )

        prev3 = points[i - 3] if i >= 3 else None
        point["live_seq_delta_3"] = diff_int(
            point.get("live_monitor_last_seq"),
            prev3.get("live_monitor_last_seq") if prev3 else None,
        )
        point["intake_seq_delta_3"] = diff_int(
            point.get("intake_cursor_last_seq"),
            prev3.get("intake_cursor_last_seq") if prev3 else None,
        )
        point["post_eval_seq_delta_3"] = diff_int(
            point.get("post_eval_max_last_seen_seq"),
            prev3.get("post_eval_max_last_seen_seq") if prev3 else None,
        )
        point["gap_delta_3"] = diff_int(
            point.get("seq_gap_live_minus_intake"),
            prev3.get("seq_gap_live_minus_intake") if prev3 else None,
        )

    latest = points[-1]

    status = "active"
    live_delta_3 = latest.get("live_seq_delta_3")
    intake_delta_3 = latest.get("intake_seq_delta_3")
    post_eval_delta_3 = latest.get("post_eval_seq_delta_3")
    gap_delta_3 = latest.get("gap_delta_3")
    cursor_age = latest.get("cursor_updated_age_seconds")
    post_eval_age = latest.get("post_eval_updated_age_seconds")

    if live_delta_3 is not None and live_delta_3 > 15000:
        intake_ok = intake_delta_3 is not None and intake_delta_3 > 1000
        post_eval_ok = post_eval_delta_3 is not None and post_eval_delta_3 > 1000

        if not intake_ok and not post_eval_ok:
            status = "stalled"
        elif gap_delta_3 is not None and gap_delta_3 < 0:
            status = "catching_up"
        else:
            status = "active"

    if cursor_age is not None and cursor_age > 900 and post_eval_age is not None and post_eval_age > 180:
        status = "stalled"

    return {
        "generated_at_utc": iso_utc(int(time.time())),
        "range_seconds": range_seconds,
        "points": points,
        "latest": latest,
        "status": status,
        "source_file": str(path),
    }


def html_page() -> str:
    return """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Live Firehose + Intake Dashboard</title>
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
  .section {
    margin-top: 26px;
    margin-bottom: 10px;
    font-size: 20px;
    font-weight: 700;
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
  <h1>Live Firehose + Intake Dashboard</h1>
  <div class="sub">One screen for live firehose volume and intake-vs-head behavior. Refreshes every second.</div>

  <div class="toolbar">
    <button data-range="60">1m</button>
    <button data-range="600" class="active">10m</button>
    <button data-range="3600">1h</button>
    <button data-range="86400">24h</button>
    <button data-range="604800">1w</button>
  </div>

  <div class="section">Live firehose</div>

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
    <div class="chart-sub">GIF/sec and video/sec</div>
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

  <div class="section">Intake vs firehose head</div>

  <div class="grid">
    <div class="card">
      <h3>Intake health</h3>
      <div class="big" id="intake-status">–</div>
      <div class="small" id="intake-status-detail"></div>
    </div>
    <div class="card">
      <h3>Gap to head (seq)</h3>
      <div class="big" id="intake-gap">–</div>
      <div class="small" id="intake-gap-detail"></div>
    </div>
    <div class="card">
      <h3>Evaluated rows (10m)</h3>
      <div class="big" id="intake-evaluated-10m">–</div>
      <div class="small">post_evaluation over last 10 minutes</div>
    </div>
    <div class="card">
      <h3>Labeled rows (10m)</h3>
      <div class="big" id="intake-labeled-10m">–</div>
      <div class="small">derived missing/partial labels over last 10 minutes</div>
    </div>
  </div>

  <div class="chart-card">
    <div class="chart-title">Gap to head</div>
    <div class="chart-sub">Live minus intake cursor, and live minus post_evaluation max_last_seen_seq</div>
    <div class="legend" id="legend-intake-gap"></div>
    <canvas id="chart-intake-gap" width="1200" height="260"></canvas>
  </div>

  <div class="chart-card">
    <div class="chart-title">Seq progress per sample</div>
    <div class="chart-sub">How fast live head, intake cursor, and post_evaluation seq are moving between samples</div>
    <div class="legend" id="legend-intake-progress"></div>
    <canvas id="chart-intake-progress" width="1200" height="260"></canvas>
  </div>

  <div class="chart-card">
    <div class="chart-title">Intake throughput</div>
    <div class="chart-sub">Evaluated rows and labeled rows over the trailing 10-minute window</div>
    <div class="legend" id="legend-intake-throughput"></div>
    <canvas id="chart-intake-throughput" width="1200" height="260"></canvas>
  </div>

  <div class="chart-card">
    <div class="chart-title">Freshness / staleness</div>
    <div class="chart-sub">How old the cursor update and post_evaluation update are</div>
    <div class="legend" id="legend-intake-freshness"></div>
    <canvas id="chart-intake-freshness" width="1200" height="260"></canvas>
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
  intakeGap: [
    { key: "seq_gap_live_minus_intake", label: "live - intake cursor", color: "#ff5d73" },
    { key: "seq_gap_live_minus_post_eval", label: "live - post_evaluation seq", color: "#ffd166" },
  ],
  intakeProgress: [
    { key: "live_seq_delta", label: "live seq delta", color: "#5aa9ff" },
    { key: "intake_seq_delta", label: "intake seq delta", color: "#06d6a0" },
    { key: "post_eval_seq_delta", label: "post-eval seq delta", color: "#c77dff" },
  ],
  intakeThroughput: [
    { key: "evaluated_rows_10m", label: "evaluated rows/10m", color: "#5aa9ff" },
    { key: "labeled_rows_10m", label: "labeled rows/10m", color: "#ff5d73" },
  ],
  intakeFreshness: [
    { key: "cursor_updated_age_seconds", label: "cursor age (s)", color: "#ffd166" },
    { key: "post_eval_updated_age_seconds", label: "post-eval age (s)", color: "#7ae582" },
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
  const pad = { top: 14, right: 18, bottom: 26, left: 58 };

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
    ctx.fillText(String((yMax - (yMax * i) / 4).toFixed(0)), pad.left - 6, yy + 4);
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
    const iso = points[idx].ts || points[idx].ts_iso || "";
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

async function fetchIntakeSeries(rangeSeconds) {
  const resp = await fetch(`/api/intake_series?range_seconds=${rangeSeconds}`);
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
}

function renderIntake(intakePayload) {
  const latest = intakePayload.latest || {};
  const points = intakePayload.points || [];
  const status = intakePayload.status || "unavailable";

  q("intake-status").textContent = status;
  q("intake-status-detail").textContent =
    `cursor_age=${fmtRate(latest.cursor_updated_age_seconds)}s post_eval_age=${fmtRate(latest.post_eval_updated_age_seconds)}s`;

  q("intake-gap").textContent = fmt(latest.seq_gap_live_minus_intake);

  let gapDetail = "–";
  if (points.length >= 2) {
    const firstGap = Number(points[0].seq_gap_live_minus_intake || 0);
    const lastGap = Number(points[points.length - 1].seq_gap_live_minus_intake || 0);
    const delta = lastGap - firstGap;
    gapDetail = `change over range: ${delta >= 0 ? "+" : ""}${fmt(delta)} seq`;
  }
  q("intake-gap-detail").textContent = gapDetail;

  q("intake-evaluated-10m").textContent = fmt(latest.evaluated_rows_10m);
  q("intake-labeled-10m").textContent = fmt(latest.labeled_rows_10m);

  drawChart("chart-intake-gap", points, SERIES.intakeGap);
  drawChart("chart-intake-progress", points, SERIES.intakeProgress);
  drawChart("chart-intake-throughput", points, SERIES.intakeThroughput);
  drawChart("chart-intake-freshness", points, SERIES.intakeFreshness);

  const footerBits = [
    `dashboard_updated=${new Date().toISOString()}`,
    `live_last_message=${latest.ts || "–"}`,
    `intake_cursor=${latest.intake_cursor_last_seq ?? "–"}`,
    `live_head=${latest.live_monitor_last_seq ?? "–"}`,
  ];
  q("footer").textContent = footerBits.join(" | ");
}

function setActiveRangeButton() {
  document.querySelectorAll("button[data-range]").forEach((btn) => {
    if (Number(btn.dataset.range) === currentRange) btn.classList.add("active");
    else btn.classList.remove("active");
  });
}

async function refreshAll() {
  try {
    const [summary, seriesPayload, intakePayload] = await Promise.all([
      fetchSummary(),
      fetchSeries(currentRange),
      fetchIntakeSeries(currentRange),
    ]);

    renderSummary(summary);

    const points = seriesPayload.points || [];
    drawChart("chart-volume", points, SERIES.volume);
    drawChart("chart-access", points, SERIES.access);
    drawChart("chart-media", points, SERIES.media);

    renderIntake(intakePayload);
  } catch (err) {
    console.error(err);
  }
}

setLegend("legend-volume", SERIES.volume);
setLegend("legend-access", SERIES.access);
setLegend("legend-media", SERIES.media);
setLegend("legend-intake-gap", SERIES.intakeGap);
setLegend("legend-intake-progress", SERIES.intakeProgress);
setLegend("legend-intake-throughput", SERIES.intakeThroughput);
setLegend("legend-intake-freshness", SERIES.intakeFreshness);
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
    intake_metrics_jsonl: str

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

        if parsed.path == "/api/intake_series":
            qs = parse_qs(parsed.query)
            try:
                range_seconds = int((qs.get("range_seconds") or ["600"])[0])
            except Exception:
                range_seconds = 600

            range_seconds = max(60, min(range_seconds, 604800))
            self._send_json(
                build_intake_series_payload(
                    self.intake_metrics_jsonl,
                    range_seconds=range_seconds,
                )
            )
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
        "--intake-metrics-jsonl",
        default="metrics/intake_head_timeseries.jsonl",
        help="JSONL file written by the intake/head metrics collector",
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
            "intake_metrics_jsonl": args.intake_metrics_jsonl,
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
                "intake_metrics_jsonl": args.intake_metrics_jsonl,
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