from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


def parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def load_snapshots(path: Path) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                items.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    items.sort(key=lambda x: x.get("generated_at_utc", ""))
    return items


def filter_snapshots(snapshots: list[dict[str, Any]], hours: float | None) -> list[dict[str, Any]]:
    if not snapshots or hours is None:
        return snapshots

    latest_ts = parse_ts(snapshots[-1]["generated_at_utc"])
    cutoff = latest_ts - timedelta(hours=hours)
    return [s for s in snapshots if parse_ts(s["generated_at_utc"]) >= cutoff]


def point_from_snapshot(snapshot: dict[str, Any]) -> dict[str, Any]:
    current = snapshot.get("current_counts") or {}
    window_10m = snapshot.get("window_10m") or {}
    fresh = snapshot.get("fresh_cohorts") or {}
    pending = snapshot.get("pending_age_buckets") or {}
    process = snapshot.get("process_health") or {}
    service_record = snapshot.get("service_record") or {}
    cursor = snapshot.get("cursor_state") or {}

    fresh_2_10m = fresh.get("2-10m") or {}
    fresh_10_30m = fresh.get("10-30m") or {}

    pending_0_2m = pending.get("0-2m") or {}
    pending_2_10m = pending.get("2-10m") or {}
    pending_10_30m = pending.get("10-30m") or {}
    pending_30_60m = pending.get("30-60m") or {}
    pending_1_4h = pending.get("1-4h") or {}
    pending_4h = pending.get("4h+") or {}

    return {
        "ts": snapshot.get("generated_at_utc"),
        "labeled_rows_10m": window_10m.get("labeled_rows", 0),
        "queued_rows_10m": window_10m.get("queued_rows", 0),
        "emitted_rows_10m": window_10m.get("emitted_rows", 0),
        "verified_rows_10m": window_10m.get("verified_rows", 0),
        "verification_failed_rows_10m": window_10m.get("verification_failed_rows", 0),
        "queued_count": current.get("queued_count", 0),
        "pending_verification_count": current.get("pending_verification_count", 0),
        "verifying_count": current.get("verifying_count", 0),
        "published_count": current.get("published_count", 0),
        "verification_failed_count": current.get("verification_failed_count", 0),
        "fresh_2_10m_forced_visible_pct": fresh_2_10m.get("forced_visible_pct"),
        "fresh_10_30m_forced_visible_pct": fresh_10_30m.get("forced_visible_pct"),
        "pending_age_0_2m": pending_0_2m.get("total_count", 0),
        "pending_age_2_10m": pending_2_10m.get("total_count", 0),
        "pending_age_10_30m": pending_10_30m.get("total_count", 0),
        "pending_age_30_60m": pending_30_60m.get("total_count", 0),
        "pending_age_1_4h": pending_1_4h.get("total_count", 0),
        "pending_age_4h_plus": pending_4h.get("total_count", 0),
        "label_apply_count": process.get("label_apply_count", 0),
        "label_verify_count": process.get("label_verify_count", 0),
        "service_record_cid": service_record.get("cid"),
        "service_record_createdAt": service_record.get("createdAt"),
        "firehose_cursor_last_seq": cursor.get("firehose_cursor_last_seq"),
        "max_last_seen_seq": cursor.get("max_last_seen_seq"),
    }


def detect_service_record_changes(points: list[dict[str, Any]]) -> list[dict[str, Any]]:
    changes: list[dict[str, Any]] = []
    previous_cid: str | None = None

    for point in points:
        cid = point.get("service_record_cid")
        if not cid:
            continue
        if previous_cid is None:
            previous_cid = cid
            continue
        if cid != previous_cid:
            changes.append(
                {
                    "ts": point["ts"],
                    "cid": cid,
                    "createdAt": point.get("service_record_createdAt"),
                }
            )
            previous_cid = cid

    return changes


def build_html(*, title: str, points: list[dict[str, Any]], service_record_changes: list[dict[str, Any]]) -> str:
    latest = points[-1] if points else {}

    payload = {
        "title": title,
        "points": points,
        "service_record_changes": service_record_changes,
        "latest": latest,
    }

    data_json = json.dumps(payload, ensure_ascii=False)

    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>{title}</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
  body {{
    font-family: system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif;
    margin: 0;
    padding: 24px;
    background: #f7f7f8;
    color: #111;
  }}
  h1 {{
    margin: 0 0 8px 0;
    font-size: 28px;
  }}
  .sub {{
    color: #555;
    margin-bottom: 20px;
  }}
  .grid {{
    display: grid;
    grid-template-columns: repeat(4, minmax(0, 1fr));
    gap: 12px;
    margin-bottom: 20px;
  }}
  .card {{
    background: white;
    border: 1px solid #e5e7eb;
    border-radius: 14px;
    padding: 14px 16px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.05);
  }}
  .card h3 {{
    margin: 0 0 6px 0;
    font-size: 13px;
    color: #555;
    font-weight: 600;
  }}
  .big {{
    font-size: 28px;
    font-weight: 700;
  }}
  .chart-card {{
    background: white;
    border: 1px solid #e5e7eb;
    border-radius: 14px;
    padding: 14px 16px 10px 16px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.05);
    margin-bottom: 16px;
  }}
  .chart-title {{
    font-size: 16px;
    font-weight: 700;
    margin-bottom: 4px;
  }}
  .chart-sub {{
    color: #666;
    font-size: 13px;
    margin-bottom: 10px;
  }}
  .legend {{
    display: flex;
    flex-wrap: wrap;
    gap: 14px;
    margin-bottom: 10px;
    font-size: 12px;
    color: #444;
  }}
  .legend-item {{
    display: inline-flex;
    align-items: center;
    gap: 6px;
  }}
  .swatch {{
    width: 10px;
    height: 10px;
    border-radius: 999px;
    display: inline-block;
  }}
  svg {{
    width: 100%;
    height: 260px;
    display: block;
    background: #fff;
  }}
  .footer {{
    color: #666;
    font-size: 12px;
    margin-top: 10px;
  }}
  .mono {{
    font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
  }}
</style>
</head>
<body>
  <h1>{title}</h1>
  <div class="sub">Minute snapshots from JSONL collector. Vertical red markers indicate service-record CID changes.</div>

  <div class="grid">
    <div class="card">
      <h3>Latest emitted rows (10m)</h3>
      <div class="big" id="card-emitted"></div>
    </div>
    <div class="card">
      <h3>Latest verified rows (10m)</h3>
      <div class="big" id="card-verified"></div>
    </div>
    <div class="card">
      <h3>Pending verification</h3>
      <div class="big" id="card-pending"></div>
    </div>
    <div class="card">
      <h3>Fresh 2–10m forced-visible %</h3>
      <div class="big" id="card-freshpct"></div>
    </div>
  </div>

  <div class="chart-card">
    <div class="chart-title">10-minute flow</div>
    <div class="chart-sub">Labeled → queued → emitted → verified</div>
    <div class="legend" id="legend-flow"></div>
    <svg id="chart-flow"></svg>
  </div>

  <div class="chart-card">
    <div class="chart-title">Backlog / state totals</div>
    <div class="chart-sub">Queued, pending verification, published, verification failed</div>
    <div class="legend" id="legend-backlog"></div>
    <svg id="chart-backlog"></svg>
  </div>

  <div class="chart-card">
    <div class="chart-title">Fresh visibility rate</div>
    <div class="chart-sub">Forced-visible percentage for 2–10m and 10–30m cohorts</div>
    <div class="legend" id="legend-fresh"></div>
    <svg id="chart-fresh"></svg>
  </div>

  <div class="chart-card">
    <div class="chart-title">Pending age buckets</div>
    <div class="chart-sub">Where the pending/verifying backlog is accumulating</div>
    <div class="legend" id="legend-pending-age"></div>
    <svg id="chart-pending-age"></svg>
  </div>

  <div class="chart-card">
    <div class="chart-title">Worker counts</div>
    <div class="chart-sub">Apply and verify workers running</div>
    <div class="legend" id="legend-workers"></div>
    <svg id="chart-workers"></svg>
  </div>

  <div class="footer mono" id="footer"></div>

<script>
const DATA = {data_json};

function fmt(n) {{
  if (n === null || n === undefined) return "–";
  return Number(n).toLocaleString();
}}

function fmtPct(n) {{
  if (n === null || n === undefined) return "–";
  return `${{Number(n).toFixed(1)}}%`;
}}

function el(id) {{
  return document.getElementById(id);
}}

function setCards() {{
  const latest = DATA.latest || {{}};
  el("card-emitted").textContent = fmt(latest.emitted_rows_10m);
  el("card-verified").textContent = fmt(latest.verified_rows_10m);
  el("card-pending").textContent = fmt(latest.pending_verification_count);
  el("card-freshpct").textContent = fmtPct(latest.fresh_2_10m_forced_visible_pct);

  el("footer").textContent =
    `points=${{DATA.points.length}} | latest=${{latest.ts || "?"}} | service_record_cid=${{latest.service_record_cid || "?"}}`;
}}

function buildLegend(containerId, series) {{
  const container = el(containerId);
  container.innerHTML = "";
  for (const s of series) {{
    const item = document.createElement("div");
    item.className = "legend-item";
    item.innerHTML = `<span class="swatch" style="background:${{s.color}}"></span><span>${{s.label}}</span>`;
    container.appendChild(item);
  }}
}}

function drawChart(svgId, series, opts={{}}) {{
  const svg = el(svgId);
  const width = svg.clientWidth || 1000;
  const height = svg.clientHeight || 260;
  const pad = {{ top: 14, right: 16, bottom: 26, left: 50 }};

  const W = width - pad.left - pad.right;
  const H = height - pad.top - pad.bottom;

  const points = DATA.points;
  if (!points.length) {{
    svg.innerHTML = "";
    return;
  }}

  let allY = [];
  for (const s of series) {{
    for (const p of points) {{
      const v = p[s.key];
      if (v !== null && v !== undefined) allY.push(Number(v));
    }}
  }}

  let yMin = opts.yMin !== undefined ? opts.yMin : 0;
  let yMax = opts.yMax !== undefined ? opts.yMax : Math.max(...allY, 1);
  if (yMax === yMin) yMax = yMin + 1;

  const x = (i) => pad.left + (points.length === 1 ? W / 2 : (i * W) / (points.length - 1));
  const y = (v) => pad.top + H - ((Number(v) - yMin) / (yMax - yMin)) * H;

  let html = "";

  for (let i = 0; i <= 4; i++) {{
    const yy = pad.top + (H * i) / 4;
    html += `<line x1="${{pad.left}}" y1="${{yy}}" x2="${{pad.left + W}}" y2="${{yy}}" stroke="#e5e7eb" stroke-width="1" />`;
    const val = yMax - ((yMax - yMin) * i) / 4;
    const label = opts.percent ? `${{val.toFixed(0)}}%` : Math.round(val).toLocaleString();
    html += `<text x="${{pad.left - 8}}" y="${{yy + 4}}" text-anchor="end" font-size="11" fill="#666">${{label}}</text>`;
  }}

  html += `<line x1="${{pad.left}}" y1="${{pad.top + H}}" x2="${{pad.left + W}}" y2="${{pad.top + H}}" stroke="#cbd5e1" stroke-width="1" />`;
  html += `<line x1="${{pad.left}}" y1="${{pad.top}}" x2="${{pad.left}}" y2="${{pad.top + H}}" stroke="#cbd5e1" stroke-width="1" />`;

  const tickIndexes = [0, Math.floor((points.length - 1) / 2), points.length - 1].filter((v, i, a) => a.indexOf(v) === i);
  for (const idx of tickIndexes) {{
    const xx = x(idx);
    const ts = new Date(points[idx].ts);
    const label = ts.toISOString().slice(11, 16);
    html += `<line x1="${{xx}}" y1="${{pad.top + H}}" x2="${{xx}}" y2="${{pad.top + H + 4}}" stroke="#94a3b8" stroke-width="1" />`;
    html += `<text x="${{xx}}" y="${{pad.top + H + 18}}" text-anchor="middle" font-size="11" fill="#666">${{label}}</text>`;
  }}

  const changes = DATA.service_record_changes || [];
  for (const change of changes) {{
    const idx = points.findIndex(p => p.ts >= change.ts);
    if (idx >= 0) {{
      const xx = x(idx);
      html += `<line x1="${{xx}}" y1="${{pad.top}}" x2="${{xx}}" y2="${{pad.top + H}}" stroke="#dc2626" stroke-width="1.5" stroke-dasharray="5 4" />`;
    }}
  }}

  for (const s of series) {{
    let d = "";
    let started = false;
    for (let i = 0; i < points.length; i++) {{
      const v = points[i][s.key];
      if (v === null || v === undefined) {{
        started = false;
        continue;
      }}
      const cmd = started ? "L" : "M";
      d += `${{cmd}} ${{x(i)}} ${{y(v)}} `;
      started = true;
    }}
    html += `<path d="${{d.trim()}}" fill="none" stroke="${{s.color}}" stroke-width="2.25" />`;
  }}

  svg.setAttribute("viewBox", `0 0 ${{width}} ${{height}}`);
  svg.innerHTML = html;
}}

setCards();

const flowSeries = [
  {{ key: "labeled_rows_10m", label: "labeled", color: "#2563eb" }},
  {{ key: "queued_rows_10m", label: "queued", color: "#7c3aed" }},
  {{ key: "emitted_rows_10m", label: "emitted", color: "#ea580c" }},
  {{ key: "verified_rows_10m", label: "verified", color: "#16a34a" }},
];
buildLegend("legend-flow", flowSeries);
drawChart("chart-flow", flowSeries);

const backlogSeries = [
  {{ key: "queued_count", label: "queued total", color: "#2563eb" }},
  {{ key: "pending_verification_count", label: "pending verification", color: "#dc2626" }},
  {{ key: "published_count", label: "published", color: "#16a34a" }},
  {{ key: "verification_failed_count", label: "verification failed", color: "#6b7280" }},
];
buildLegend("legend-backlog", backlogSeries);
drawChart("chart-backlog", backlogSeries);

const freshSeries = [
  {{ key: "fresh_2_10m_forced_visible_pct", label: "2–10m forced-visible %", color: "#16a34a" }},
  {{ key: "fresh_10_30m_forced_visible_pct", label: "10–30m forced-visible %", color: "#ea580c" }},
];
buildLegend("legend-fresh", freshSeries);
drawChart("chart-fresh", freshSeries, {{ yMin: 0, yMax: 100, percent: true }});

const pendingAgeSeries = [
  {{ key: "pending_age_0_2m", label: "0–2m", color: "#16a34a" }},
  {{ key: "pending_age_2_10m", label: "2–10m", color: "#84cc16" }},
  {{ key: "pending_age_10_30m", label: "10–30m", color: "#f59e0b" }},
  {{ key: "pending_age_30_60m", label: "30–60m", color: "#ea580c" }},
  {{ key: "pending_age_1_4h", label: "1–4h", color: "#dc2626" }},
  {{ key: "pending_age_4h_plus", label: "4h+", color: "#7c2d12" }},
];
buildLegend("legend-pending-age", pendingAgeSeries);
drawChart("chart-pending-age", pendingAgeSeries);

const workerSeries = [
  {{ key: "label_apply_count", label: "apply workers", color: "#2563eb" }},
  {{ key: "label_verify_count", label: "verify workers", color: "#7c3aed" }},
];
buildLegend("legend-workers", workerSeries);
drawChart("chart-workers", workerSeries, {{ yMin: 0 }});
</script>
</body>
</html>
"""


def main() -> None:
    parser = argparse.ArgumentParser(description="Render HTML dashboard from collected pipeline JSONL snapshots.")
    parser.add_argument(
        "--input-jsonl",
        default="metrics/pipeline_timeseries.jsonl",
        help="Input JSONL snapshots file",
    )
    parser.add_argument(
        "--output-html",
        default="metrics/pipeline_dashboard.html",
        help="Output HTML file",
    )
    parser.add_argument(
        "--hours",
        type=float,
        default=2.0,
        help="Include only the last N hours of snapshots. Use 0 or negative to include all.",
    )
    parser.add_argument(
        "--title",
        default="Alt-Text Labeler Pipeline Dashboard",
        help="Dashboard title",
    )
    args = parser.parse_args()

    input_path = Path(args.input_jsonl)
    output_path = Path(args.output_html)

    snapshots = load_snapshots(input_path)
    if not snapshots:
        raise SystemExit(f"No snapshots found in {input_path}")

    hours = args.hours if args.hours and args.hours > 0 else None
    filtered = filter_snapshots(snapshots, hours)
    points = [point_from_snapshot(s) for s in filtered]
    service_record_changes = detect_service_record_changes(points)

    html = build_html(
        title=args.title,
        points=points,
        service_record_changes=service_record_changes,
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html, encoding="utf-8")

    print(
        json.dumps(
            {
                "generated_at_utc": datetime.now(timezone.utc).isoformat(),
                "input_jsonl": str(input_path),
                "output_html": str(output_path),
                "points_included": len(points),
                "service_record_changes_included": len(service_record_changes),
                "first_point": points[0]["ts"] if points else None,
                "last_point": points[-1]["ts"] if points else None,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()