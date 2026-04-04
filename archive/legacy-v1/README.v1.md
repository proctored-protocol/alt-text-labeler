# alt-labeler

A Bluesky accessibility labeler focused on still-image alt text.

This project evaluates image posts on the AT Protocol firehose and applies descriptive labels through an Ozone-backed labeler service.

## Current status

The following parts are working:

- firehose ingestion
- still-image embed parsing
- post-level evaluation
- PostgreSQL persistence
- Ozone-backed manual and scripted label publication
- labeler service registration and discoverability

Known external blocker:

- labels are successfully published in Ozone and the labeler service is discoverable
- however, Bluesky AppView is not currently including this labeler in `atproto-content-labelers` for tested content responses
- because of that, subscribed end users may not yet see labels in Bluesky clients even though publication succeeds

Because of this, the project is currently in a **staging** state rather than a full live rollout state.

## v1 scope

This repository currently supports:

- post-level evaluation only
- still-image posts only
- factual labels only:
  - `missing-alt-text`
  - `partial-alt-text`

It does not currently support:

- account-level automated scoring
- video/GIF evaluation
- semantic alt-text quality judgment
- production rollout to end users while AppView visibility remains blocked

## Label semantics

### `missing-alt-text`
Applied when a post contains one or more still images and none of them has usable alt text.

### `partial-alt-text`
Applied when a post contains multiple still images and only some of them have usable alt text.

For v1, usable alt text means:
- alt field present
- non-empty after trimming whitespace

## Architecture

### Python worker
The Python worker:
- listens to the Bluesky firehose
- extracts still-image posts
- evaluates alt coverage
- stores results in PostgreSQL
- queues or publishes labels

### Ozone
Ozone is the actual moderation/label-emitting service.

The worker authenticates via a Bluesky session, then sends moderation actions through the Ozone/AppView path using:
- `Authorization: Bearer <accessJwt>`
- `atproto-proxy: <labeler did>#atproto_labeler`

## Repo layout

### Core application
- `app/firehose/` — firehose handling
- `app/parsing/` — post/embed parsing
- `app/rules/` — evaluation rules
- `app/services/` — cursor/evaluator/override services
- `app/integrations/ozone/` — Ozone/AppView bridge

### Scripts
- `scripts/run_worker.py` — run the firehose worker
- `scripts/show_recent.py` — inspect recent evaluated posts
- `scripts/show_counts.py` — summary of evaluations
- `scripts/show_publication_counts.py` — summary of publication rows
- `scripts/test_ozone_auth.py` — verify Ozone/AppView auth path
- `scripts/publish_one_to_ozone.py` — manually publish one label
- `scripts/check_post_labels.py` — inspect whether AppView returns labels for a post
- `scripts/run_api.py` — small local health/version API
- `scripts/register_labeler_service.py` — write the labeler service self record

### Docs
- `docs/deployment.md`
- `docs/ozone-setup.md`

## Local development

### Requirements
- Python 3.11+
- Docker
- PostgreSQL via Docker Compose

### Setup

1. Copy `.env.example` to `.env`
2. Start local Postgres:

```bash
docker compose up -d
```

3. Create a virtual environment and install:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
```
4. Run tests:

```bash
pytest
```

## Useful commands

### Run the worker

```bash
python scripts/run_worker.py
```

### Inspect evaluations

```bash
python scripts/show_recent.py
python scripts/show_counts.py
```

### Inspect publication rows

```bash
python scripts/show_publication_counts.py
```

### Check Ozone/AppView auth

```bash
python scripts/test_ozone_auth.py
```

### Publish one label manually

```bash
python scripts/publish_one_to_ozone.py "<URI>" "<CID>" "missing-alt-text"
```

### Check whether AppView surfaces labels on a post

```bash
python scripts/check_post_labels.py "<AT_URI>"
```

### VPS Monitoring Commands

```bash
journalctl -u alt-text-labeler-worker -f
```

```bash
cd /srv/alt-text-labeler
source .venv/bin/activate
python scripts/show_counts.py
python scripts/show_publication_counts.py
```

## Configuration

### Key local environment variables

- FIREHOSE_BASE_URI
- FIREHOSE_DRY_RUN
- DATABASE_URL
- LABEL_MISSING_ALT
- LABEL_PARTIAL_ALT
- BSKY_HANDLE
- BSKY_APP_PASSWORD
- BSKY_PDS_URL
- OZONE_BASE_URL
- OZONE_PROXY_DID
- OZONE_HANDLE
- OZONE_APP_PASSWORD
- PUBLISH_VIA_OZONE

Optional:

- TEST_VIEWER_HANDLE
- TEST_VIEWER_APP_PASSWORD

### Current rollout recommendation

Keep the project in staging mode for now:

- use manual Ozone labeling and targeted script-based tests

- keep automatic publishing disabled or conservative

- periodically re-check whether AppView includes the labeler in atproto-content-labelers

Once AppView starts surfacing labels consistently, the next steps are:

- enable live publishing from the worker

- consider stronger semantics for missing-alt-text if desired

- begin cautious rollout to real subscribers
