<instruction>You are an expert software engineer. You are working on a WIP branch. Please run `git status` and `git diff` to understand the changes and the current state of the code. Analyze the workspace context and complete the mission brief.</instruction>
<workspace_context>
<artifacts>
--- CURRENT TASK CHECKLIST ---
# Solana Analytics - Ingestion Hardening Implementation

## Phase 1: Schema Documentation
- [x] Create `schema/001_initial_schema.sql` - Full table DDL with constraints
- [x] Create `schema/002_add_ignored_reasons.sql` - Add 5 counter columns

## Phase 2: Schema Tools
- [x] Create `tools/verify_schema.py` - Validates DB matches expected state
- [x] Create `tools/migrate_schema.py` - Runs migrations safely

## Phase 3: Code Fixes
- [x] Fix `api/webhooks.py` (7 changes)
  - Replace single counter with 5 detailed counters
  - Collect all swap legs before inserting
  - Update constraint to composite key
  - Track which legs succeed/fail
  - Enhanced logging with breakdown
  - Add `ignored_reasons` to response
- [x] Fix `backfill_solana.py` (4 changes)
  - Update constraint to composite key
  - Add tracking counters
  - Log duplicates vs new inserts
  - Summary statistics
- [x] Enhance `api/metrics.py` (2 new endpoints)
  - `GET /metrics/ingestion-stats` - Detailed breakdown
  - `GET /metrics/ingestion-health` - Health monitoring

## Phase 4: Testing
- [x] Create `tests/test_idempotency.py` - Test replay protection and deduplication
- [x] Create `tests/test_constraint_enforcement.py` - Direct DB constraint tests
- [x] Create `tests/test_load.py` - Concurrent request testing

## Phase 5: Deployment
- [x] Run schema verification
- [x] Apply database migrations
- [x] Deploy code to Fly.io
- [x] Verify health endpoints
- [x] Confirm multi-leg swaps working
</artifacts>
</workspace_context>
<mission_brief>[Describe your task here...]</mission_brief>