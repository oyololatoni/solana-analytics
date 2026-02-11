# Operational Manual & Principles

This document outlines the core operational principles for maintaining the Solana Analytics pipeline. These are hard rules, not suggestions.

## 1. Pre-deploy Safety Guarantees (The Seatbelt)

**Rule:** Nothing deploys without passing `preflight.py` first.

We have enforced this in `tools/deploy.py`. The script will now **abort immediately** if:
*   Required environment variables (`DATABASE_URL`, `SECRET`, `TOKENS`) are missing.
*   Database connection fails OR required tables (`events`, `raw_webhooks`) are missing.
*   Critical code modules fail to import (Syntax errors, missing dependencies).

**Your Workflow:**
1.  Run `python preflight.py` (or `tools/deploy.py --dry-run`) locally.
2.  Trust the CI/CD pipeline which runs the **exact same gate** via `tools/deploy.py --db-only`.

## 2. Ingestion Control Plane

**Rule:** Use `INGESTION_ENABLED` as a runtime switch.

*   **Status Quo:** Default is `1` (Enabled).
*   **Incident Response:** If the DB is overwhelmed or you are debugging bad data, set `INGESTION_ENABLED=0` in Fly.io secrets.
*   **Effect:** Webhooks verify signature and payload but **do not write to the DB**. This protects your data integrity while keeping Helius happy (200 OK).

## 3. Replay Protection & Visibility

**Rule:** "Ignored" is not enough. We must know *why*.

We now track granular ignore reasons in the `ingestion_stats` table:
*   `ignored_missing_fields`: Bad payload shape.
*   `ignored_no_swap_event`: Not a swap transaction.
*   `ignored_no_tracked_tokens`: Swap didn't involve our tokens (checking inputs & outputs).
*   `ignored_constraint_violation`: Duplicate event (Idempotency protection working).
*   `ignored_exception`: Code crash (Needs immediate investigation).

**Verification:**  
Check `GET /metrics/ingestion-stats` to see these counters in real-time.

## 4. Environment Truth

**Rule:** Never assume Local == Remote.

*   You configured `TRACKED_TOKENS` locally? **Verify it on Fly.**
*   You updated the DB URL? **Verify it on Fly.**

The deployment script now runs a **Post-Deployment Verification** step that hits:
*   `https://<app>.fly.dev/health` -> Confirms Remote DB connectivity.
*   `https://<app>.fly.dev/metrics/ingestion-stats` -> Confirms config is loaded and API is responsive.

## 5. Operational Feedback Loop

**Rule:** Shift Left. Test locally first.

**Old Loop (Slow):** Change -> Deploy -> Wait -> Log -> Fix  
**New Loop (Fast):** Change -> `tools/replay_webhook.py` -> Verify Local DB -> Deploy

Use `tools/replay_webhook.py` to simulate Helius events against your local running server (`localhost:8000`). Only deploy when you see the expected result locally.

## 6. Project Ergonomics

**Rule:** GitHub is the Source of Truth.

*   No manual file uploads.
*   All changes go through PRs / Commits to `main`.
*   CI checks (`.github/workflows/ci.yml`) enforce quality before you even think about deploying.
