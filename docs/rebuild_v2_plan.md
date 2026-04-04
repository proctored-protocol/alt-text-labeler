# Alt-Text Labeler v2 Rebuild Plan

## Overall objective

Build a labeler that:

1. scans the full Bluesky firehose,
2. identifies posts with images and missing/partial alt text,
3. applies and publishes labels with minimal end-to-end lag,
4. verifies visibility in AppView,
5. remains robust under interruptions,
6. reports trustworthy metrics for every stage,
7. supports health monitoring, auto-recovery, autoscaling, and catch-up mode.

## End-state success criteria

### Functional
- Every relevant post entering the firehose can be:
  - observed,
  - ingested,
  - evaluated,
  - published,
  - verified.

### Performance
- Intake should stay as close as possible to the firehose head.
- Target intake lag: roughly 30–60 seconds behind head.
- Target end-to-end latency from post creation to confirmed AppView visibility:
  - acceptable: under 2 minutes
  - ideal: under 1 minute

### Operational
- Every stage has:
  - a clear responsibility,
  - canonical state,
  - canonical metrics,
  - restart behavior,
  - backlog visibility,
  - health signals.

### Engineering discipline
- One worker, one responsibility.
- No experimental scripts in the active runtime path.
- No ambiguous or proxy metrics presented as authoritative.
- No runtime tables created only by ad hoc scripts.
- Anything experimental lives under `archive/` or a separate branch.

---

## Current status

### Current phase
**Phase 4 — Apply**

### Completed
- Legacy v1 runtime was stopped on the server.
- Project database on the server was wiped.
- Repository was archived into `archive/legacy-v1/`.
- Active branch switched to `rebuild-v2`.
- Legacy script sprawl removed from the active runtime path.
- Rebuild architecture agreed at a high level.
- Canonical `config.py`, `db.py`, and `models.py` were created.
- Alembic migration path was set up.
- Initial canonical v2 schema migration was applied successfully.
- Canonical tables were verified in Postgres.
- Head tracker storage layer (`consumer_state`, `firehose_head_sample`) was implemented.
- Head tracker runtime was implemented and tested.
- Head tracker was verified to:
  - write continuous per-second samples,
  - maintain correct consumer state,
  - restart cleanly,
  - report zero gap to head for itself,
  - maintain low freshness values while running.
- Intake storage and runtime were implemented against the canonical schema.
- Intake was verified to:
  - maintain its own durable cursor,
  - persist relevant image posts into `intake_item`,
  - stay live over extended runs,
  - produce believable lag metrics against the live head tracker,
  - recover from lag spikes without evidence of runaway drift.

### Next immediate step
Implement apply:
- lease pending `intake_item` rows,
- evaluate them using the canonical ruleset,
- materialize results in `label_decision`,
- create `publish_job` rows only when publication is required,
- keep apply fully separate from publish and visibility verification.

---

## Canonical architecture

### A. Head tracker
Purpose:
- stay at the firehose front,
- record `(seq, observed_at)` continuously,
- provide the authoritative lag reference.

Outputs:
- `firehose_head_sample`
- `consumer_state` entry for head tracker

Primary metric:
- intake lag in seconds

### B. Intake
Purpose:
- consume from its own cursor,
- store only relevant posts with images,
- do **not** apply rules here.

Outputs:
- `intake_item`

Primary metrics:
- intake items/sec
- lag vs head
- cursor freshness

### C. Apply
Purpose:
- evaluate stored intake items against the ruleset,
- materialize label decisions.

Outputs:
- `label_decision`

Primary metrics:
- evaluated/sec
- labeled/sec
- queue depth
- queue age

### D. Publish
Purpose:
- publish decisions externally via Ozone or replacement path,
- avoid unnecessary external API usage,
- do **not** verify visibility here.

Outputs:
- `publish_job`
- `publish_attempt`

Primary metrics:
- accepted/sec
- retry rate
- external error buckets
- backlog age

### E. Visibility
Purpose:
- verify forced-hydration visibility,
- compute end-to-end latency.

Outputs:
- `visibility_check`

Primary metrics:
- time to visibility
- success rate
- verification backlog age

### F. Control plane
Purpose:
- health monitoring,
- watchdog restarts,
- autoscaling,
- catch-up mode,
- control actions log.

Outputs:
- `worker_heartbeat`
- `control_action_log`

---

## Canonical runtime tables

These are the only authoritative runtime tables for v2.

- `consumer_state`
- `firehose_head_sample`

- `intake_item`

- `label_decision`

- `publish_job`
- `publish_attempt`

- `visibility_check`

- `worker_heartbeat`
- `control_action_log`

- `manual_override`

Optional later:
- `metric_rollup_minute`

Not part of initial stabilization.

---

## Fixed rebuild phases

### Phase 0 — Freeze and archive
Goal:
- stop all runtime,
- archive legacy scripts,
- reduce active code surface,
- remove bogus metrics from active use.

Exit criteria:
- no old workers running,
- cleaned branch active,
- legacy code moved out of active runtime path.

### Phase 1 — Schema first
Goal:
- define canonical tables,
- define stage boundaries in DB terms,
- define indexes and state transitions.

Why first:
- all workers depend on state shape.

Exit criteria:
- schema agreed,
- migrations created,
- clean bootstrap works.

### Phase 2 — Head tracker
Goal:
- implement authoritative head tracker,
- compute true lag in seconds.

Exit criteria:
- head samples stored continuously,
- head freshness visible,
- lag query works.

### Phase 3 — Intake
Goal:
- rebuild intake as pure intake,
- no rule application inside intake,
- support catch-up mode.

Exit criteria:
- intake cursor advances,
- lag can shrink after restart,
- only relevant image posts are stored.

### Phase 4 — Apply
Goal:
- evaluate rules in a separate leased stage,
- write canonical label decisions.

Exit criteria:
- deterministic decisions,
- idempotent processing,
- visible backlog and throughput.

### Phase 5 — Publish
Goal:
- publish label decisions with retries/backoff,
- minimize external API usage.

Exit criteria:
- publish backlog visible,
- retries categorized,
- throughput measurable.

### Phase 6 — Visibility
Goal:
- verify forced hydration,
- compute latency per post and in aggregate.

Exit criteria:
- correct latency metrics,
- no bogus proxy metrics.

### Phase 7 — Control plane
Goal:
- watchdog,
- auto-restart,
- autoscaling,
- catch-up mode control.

Exit criteria:
- induced stall is detected,
- induced stall recovers automatically,
- scaling decisions use canonical metrics.

### Phase 8 — Final dashboard
Goal:
- one coherent dashboard,
- only canonical metrics,
- clear definitions for every number and line.

Exit criteria:
- every displayed metric maps to a named query,
- head-relative lag is correct,
- time windows are explicit and correctly interpreted.

---

## Code review order

We review and rebuild in this order.

1. `app/config.py`
2. `app/db.py`
3. `app/models.py`
4. `app/parsing/embeds.py`
5. `app/parsing/posts.py`
6. `app/rules/alt_text.py`
7. `app/rules/labeling.py`
8. `app/head/*`
9. `app/intake/*`
10. `app/apply/*`
11. `app/publish/*`
12. `app/visibility/*`
13. `app/control/*`
14. `app/api/*`
15. tests
16. systemd units

This order is mandatory unless explicitly changed in this file.

---

## Working rules during rebuild

1. One worker, one responsibility.
2. No ad hoc scripts in active `scripts/`.
3. Every metric must map to a canonical query or function.
4. Every stage must have explicit state and transitions.
5. No direct external API call from the wrong stage.
6. No misleading labels in metrics or dashboard UI.
7. No new runtime tables outside canonical schema management.
8. Experimental work goes to `archive/` or a separate branch.
9. Changes to architecture must be reflected in this file.
10. Debugging loops must not silently change scope.

---

## Open questions / risks

These remain open until explicitly resolved.

1. What publish throughput can Ozone sustain for this workload?
2. What end-to-end latency is realistically achievable at target scale?
3. Will publish need an alternate path beyond the current Ozone approach?
4. What autoscaling signals are sufficient and stable for each stage?
5. What restart policy is safe for long-running workers under load?

---

## Change log

### 2026-04-04
- Created v2 rebuild plan.
- Agreed to use this file as the external source of truth.
- Current phase set to Phase 1 — Schema first.
- Advanced from Phase 1 — Schema first to Phase 2 — Head tracker after successful canonical schema migration and verification.
- Advanced from Phase 2 — Head tracker to Phase 3 — Intake after validating continuous per-second head sampling, restart behavior, and correct zero-gap lag reporting for the head tracker.
- Advanced from Phase 3 — Intake to Phase 4 — Apply after validating long-running intake behavior, durable cursor updates, live `intake_item` writes, and believable lag/recovery behavior against the head tracker.