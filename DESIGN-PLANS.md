# Design plans — deeper refactors from FABLE-REVIEW.md

Date: 2026-07-09. Plan only — no code changes. Needs a go/no-go before implementation.

**Deferred to Sunday's Fable cross-verse design session, not covered here:** C1+H1+H3+M7 (rework cricsheet-daily publish into a staged, verified, manifest-last upload with changed-match detection). This is a facet of the same atomic-publish problem torp and panna also have — see `C:\dev\FABLE-BURN.md` Prompt F.

---

## H4 + M3 — Pipeline state machine (per-step status, skip logic, connection hygiene)

**Problem:** the 15-step pipeline's failure handling and skip logic don't talk to each other.

- Steps 12-15 (`data-raw/run_full_pipeline.R:749-878`) wrap their work in warn-only `tryCatch`, then unconditionally write `status = "success"` regardless of whether anything inside actually failed. The completion banner says "Pipeline Complete" even when every in-match model training failed.
- `should_skip_step()`/`pipeline_should_run()` (`R/pipeline_state.R:135-153,206-213,283-285`) bases skip decisions purely on net row-count deltas and **never consults `status` at all**. A delete+re-add of a changed Test match nets ~0 new rows → "no new data" → the step is skipped — even if the state file says the *previous* run of that step actually failed.
- Net effect: a broken step 12 training run gets recorded as success, future runs skip retraining (smart-skip sees no new data), and steps 13-14-15 keep building on the stale/broken output indefinitely, invisibly.
- Separately, step 2 disconnects with `shutdown = FALSE` (the one place in the codebase that does this — the documented source of the Windows DuckDB lock), and `batch_load_matches` (`data_ingestion.R:524-525,630-631`) holds a read-only connection open while opening a write connection to the same file.

**Proposed approach:**
1. Make step IDs canonical strings in `pipeline_state.R` (the "5b" %in% 3:9 numeric-range bug is already fixed elsewhere this session; this generalizes the fix properly) and track a real `status` field (`"success" | "failed" | "skipped"`) per step alongside the row-count delta.
2. `should_skip_step()` must require `last_status == "success"` in addition to "no new data" before allowing a skip — a step that previously failed should never be silently skipped regardless of row counts.
3. Steps 12-15's error handlers must write `status = "failed"` on a caught error (not "success"), and must disconnect any open DuckDB connection in the error path before returning — the current handlers log and continue without cleanup.
4. Standardize `shutdown = TRUE` everywhere; fix `batch_load_matches` to close its read-only connection before opening the writer.
5. Make the completion banner reflect actual per-step status, not just "reached the end of the script."

**Risk:** this touches the orchestration of a production pipeline actively feeding match predictions. A bug in the *new* logic (e.g. a step wrongly marked "failed" when it actually succeeded) creates the opposite failure mode: unnecessary full reprocessing/retraining every run, burning real compute and time. This needs validation against real run history before going live, not just a code review.

**Verification:**
- Build a state-machine unit-test harness independent of the real DuckDB — mock state transitions (success→success, success→failed→success, failed→failed) and assert skip decisions match the intended policy in every case.
- Once implemented, run the full pipeline once locally end-to-end with a deliberately-injected failure (e.g. temporarily corrupt one input to force step 13 to fail) and confirm the banner and per-step logs accurately report the failure, rather than "Pipeline Complete."
- Replay the new skip logic against real historical pipeline_state records (if retained) to confirm it wouldn't have changed past skip/run decisions for runs that genuinely succeeded — only for the ones that should have been caught.
