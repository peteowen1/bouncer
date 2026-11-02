# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Docs

Git workflow (branch, PR-to-main) is in the global and parent CLAUDE.md, not restated here. Verse-level docs (reviews, plans, decision log, work queue) live in the parent's `bouncerverse/docs/` — start at `../docs/HOME.md`.

## Package Overview

Cricket analytics R package with **ball-by-ball player ratings** and a **15-step prediction pipeline**:

```
Steps 1-11: AGNOSTIC → SKILLS → FULL MODEL → PRE-MATCH → PROJECTIONS
Steps 12-15: IN-MATCH MODELS → PLAYER GAME DATA → STAT RATINGS → CAREER RATINGS (BOUNCER)
```

### Rating Systems (3 complementary approaches)

| System | Files | Use Case |
|--------|-------|----------|
| **3-Way ELO** | `three_way_elo.R`, `three_way_elo_tables.R` | Batter + Bowler + Venue (dual session/permanent) |
| **PageRank/Centrality** | `centrality.R`, `centrality_storage.R` | Network-based quality adjustment (detects isolated cluster inflation); feeds 3-Way ELO's K-factor and inactivity decay, not the outcome models directly |
| **Stat Ratings** | `stat_ratings.R`, `stat_rating_config.R` | Bayesian per-game stat ratings (PSR, economy, SR, etc.) |

Stat ratings feed the BOUNCER composite value system (`bouncer_rating.R`). Glicko is deprecated and archived in `data-raw/_deprecated/`.

> ### 3-Way ELO feeds the delivery-level models — but only since 2026-08-20/21
>
> A table-name bug (two readers built `paste0(format, "_3way_elo")` instead of
> using the gender-keyed name) meant every full model ever trained before that
> date had all three ELO features frozen at the neutral 1400, and
> `calculate_roster_elo()` scored any two rosters identically — while the
> pipeline printed a success line. Fixed by declaring the table name once in
> `three_way_elo_table()`; retrained on clean inputs, ELO coverage went to
> 100% and gave a matched, bootstrapped **T20 +1.80%, ODI +1.69%, Test +2.34%**
> logloss gain over the agnostic model (bouncerverse#63, #65). Whether 3-Way
> ELO is worth keeping past the ball-by-ball simulation is still an open call,
> not settled fact — see the "decide" verdict in
> `../docs/reference/RATING-ARCHITECTURE.md`. As of 2026-09-17,
> `load_full_model()`/`predict_full_outcome()` are exported and wired into
> `run_full_pipeline.R`, but whether the retrained model reaches live serving
> (not just training) hasn't been re-checked since — see
> `../docs/reference/MODEL-INVENTORY.md`. Full incident and retrain numbers:
> `../docs/reference/claude-md-detail.md`.

> ### WPA feeding the ratings is real, but it barely moves anything
>
> `batting_wpa`/`bowling_wpa` come from bouncer's own models
> (`build_cricinfo_win_probability()` / `build_cricinfo_test_win_probability()`,
> scoring every T20/ODI/Test delivery into `main.bouncer_wp_from_cricinfo`); the
> old scraped Cricinfo win probability is still selectable via
> `wp_source = "cricinfo"` for comparison. Ours wins on evidence (Brier and
> coverage) — detail and the temporal-holdout caveat on the headline Brier
> figure: `../docs/2026-08-13-SESSION-HANDOVER.md` §8.
>
> **WPA contributes ~0.01% of the variance in the rating engine's inputs — do
> not expect a WPA change to move a rating until this changes.** The engine is
> `calculate_impact()` (per-match `raa + kappa*wpa`, kappa fitted: 150 T20, 272
> ODI); `calculate_epr()` survives only as a deprecated alias. Do not "fix" the
> imbalance by standardising the two components — tested, and it made the
> anchor checks worse.
>
> **Two correctness rules that must not regress:** (1) the WPA delta is
> flipped to the batter's own team's perspective in `.wp_source_sql()` — never
> difference either WP column directly without it (summing raw deltas docked
> chasing batters for scoring). (2) `calculate_impact()`'s coverage warning
> must not be silenced — matches with no win probability reach it as `NA`;
> `.merge_batting_bowling()` used to launder those into zeros instead.
>
> Full derivation, benchmark numbers (Brier, coverage %, variance shares) and
> commit history: `../docs/reference/claude-md-detail.md`.

**Key Formula (residual-based skill updates):**
```r
residual = actual - agnostic_expected
new_skill = (1 - alpha) * old_skill + alpha * residual
```

**3-Way ELO Formula:**
```r
expected_runs = agnostic_baseline * (1 + (batter_elo + venue_elo - bowler_elo) * runs_per_100_elo)
```

## Code Organization

### R/ - Package Functions (by domain)

| Domain | Files | Purpose |
|--------|-------|---------|
| **Data** | `database_*.R`, `data_*.R`, `data_queries.R`, `cricsheet_*.R`, `manifest.R` | DB connections, queries, parsing, remote manifests |
| **Scraping** | `fox_scraper.R`, `fox_data.R`, `cricsheet_download.R` | Data acquisition (Fox Sports, Cricsheet) |
| **Cricinfo** | `cricinfo_data.R`, `cricinfo_win_probability.R`, `cricinfo_test_win_probability.R` | Cricinfo ingestion, loaders, fixtures, our own win-probability models |
| **ELO/Skills** | `three_way_elo.R`, `three_way_elo_tables.R`, `elo_utils.R`, `elo_calibration.R`, `elo_calibration_compute.R`, `team_elo.R`, `team_elo_optimization.R`, `team_correlation.R`, `*_skill_index.R`, `skill_indices.R`, `centrality.R`, `centrality_storage.R` | Rating systems |
| **Stat Ratings** | `stat_ratings.R`, `stat_rating_config.R`, `stat_rating_data.R` | Bayesian stat ratings (PSR, economy, SR, etc.) |
| **BOUNCER Value** | `bouncer_rating.R`, `player_stat_value.R`, `raa.R`, `raa_cricsheet.R`, `leverage.R` | Composite player value (PSV/BatV/BowlV → EPR → BOUNCER); `calculate_impact()`'s `raa + kappa*wpa` engine and leverage-weighted WPA — see the WPA box above |
| **Player Rating v2** | `player_rating_v2.R`, `player_rating_v2_storage.R`, `rating_validation.R` | Opponent/competition-adjusted rating tuned to predict the player's next match (D-P17); `rating_validation.R` is the forward-looking validation harness, not tied to any one rating |
| **Player Data** | `player_game_data.R`, `player_game_data_storage.R`, `player_game_ratings.R`, `player_career_ratings.R`, `player_career_display.R` | Per-game/career rating storage and display |
| **Home Advantage** | `home_advantage.R` | Home-team detection, venue-country mapping |
| **Models** | `agnostic_model.R`, `model_predictions.R`, `score_projection*.R`, `in_match_prediction.R`, `match_predictions.R`, `pre_match_features.R`, `team_predictions.R` | XGBoost models, predictions |
| **Model Validation** | `calibration_audit.R` | `calibration_audit()`/`worst_calibration_buckets()`/`audit_low_information_state()` — per-cut calibration audit and the low-information-state leak check; see `../docs/reference/MODEL-VALIDATION-PROTOCOL.md` |
| **Features** | `feature_engineering.R`, `expected_outcomes.R`, `margin_calculation.R`, `win_probability_added.R`, `match_outcomes.R`, `player_attribution.R`, `hawkeye_features.R` | Feature calculation, WPA, outcomes, Hawkeye |
| **Simulation** | `simulation.R` | Ball-by-ball match simulation |
| **User API** | `user_install.R`, `user_api.R`, `player_metrics.R`, `team_metrics.R` | Public-facing functions, stats |
| **Config** | `constants.R`, `constants_3way.R`, `constants_skill.R`, `globals.R`, `bouncer-package.R` | Constants, globals, package docs |
| **Utilities** | `format_utils.R`, `validation_helpers.R`, `pipeline_state.R`, `pipeline_benchmark.R`, `event_tiers.R`, `team_ids.R` | Helpers, validation, pipeline state, benchmarks |
| **Weather** | `weather.R` | Weather data (Open-Meteo API, venue geocoding) |
| **Tuning** | `xgb_tuning.R` | XGBoost hyperparameter tuning utilities |
| **Vendored** | `versebus.R` | Canonical copy is `torpverse/torp/R/versebus.R`; `test-versebus-sync.R` pins only the `SHARED_VERSEBUS_FUNCTIONS` list (function by function, not the whole file) to it — never hand-edit those functions, sync them from torp instead. Two constants, `VB_TRUE_SIZE_TIMEOUT_SECS` and `VB_ASSET_CONFIRMED_ABSENT`, are defined locally at the top of the file (torp keeps them in `constants_data.R`); their values must match torp's |
| **Visualization** | `visualization.R` | ggplot2-based plotting functions |

### tests/testthat/

Covers ratings, models, data pipeline, and API surfaces — 75 test files. Run `ls tests/testthat/` for the current list; run a single file with `testthat::test_file("tests/testthat/test-<name>.R")`.

### debug/ - Scratch Scripts (gitignored)

Throwaway scripts for debugging, CRAN prep, one-off checks. Everything in `debug/` is gitignored and excluded from the package tarball. Use this instead of creating temp files at the package root.

### data-raw/ - Analysis Scripts (NOT part of package)

Not part of the package. Entry point is `run_full_pipeline.R`; `ARCHITECTURE.md` has the complete technical documentation. Organized by topic (`data-acquisition/`, `debug/`, `ratings/`, `models/`, `simulation/`, `release/`, `utils/`, `validation/`, `archive/`, `_deprecated/` for retired dual-ELO/Glicko) — run `ls -R data-raw/` for the current layout. Pipeline-step mapping for files where it isn't obvious from the path: `ratings/player/skill-indices/` = Step 3, `ratings/player/3way-elo/` = Step 5b, `models/in-match/` = Step 12, `ratings/player/stat-ratings/` = Steps 13-15.

**Data location**: `../bouncerdata/` (sibling directory)

## Database Access

```r
# NEVER hardcode paths - use helper functions
conn <- get_db_connection(read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

DBI::dbGetQuery(conn, "SELECT COUNT(*) FROM cricsheet.deliveries")

# Other helpers
get_db_path()           # Full path to bouncer.duckdb
find_bouncerdata_dir()  # Path to bouncerdata/ directory
```

**DuckDB constraint**: Only ONE write connection at a time. If locked:
```r
duckdb::duckdb_shutdown(duckdb::duckdb())
```

## Debug Script Template

```r
# data-raw/debug/debug_something.R
library(DBI)
devtools::load_all()

conn <- get_db_connection(read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

# Your debug queries/code here
```

## Key Constraints

### R Package Rules
- All exported functions need `@export` AND a roxygen2 title/description (bare `@export` without a title generates a NAMESPACE entry but no man page, which breaks pkgdown)
- `_pkgdown.yml` must match NAMESPACE exactly

### Documentation (pkgdown)
- Site: https://peteowen1.github.io/bouncer/ (deployed via GitHub Actions on push to `main`)
- Logo: `man/figures/logo.png` (reproducible via `data-raw/logo/create_logo.R`)
- Every NAMESPACE export must appear in `_pkgdown.yml` reference sections — use the `check-pkgdown` skill/agent to verify alignment

### Rating Calculations
- **MUST be processed in strict chronological order** - never parallelize
- Sort by `match_date → match_id → delivery_id`

## Database Schema

Uses DuckDB schemas for namespace isolation: `cricsheet.*` for Cricsheet data, `cricinfo.*` for Cricinfo data, `main.*` for ratings/skills/predictions.

**Delivery ID Format**: `"{match_id}_{batting_team}_{innings}_{over}_{ball}"`
- Over: 3 digits zero-padded, Ball: 2 digits
- Example: `"64012_India_1_005_03"`

**match_type values**: `T20`, `IT20`, `ODI`, `ODM`, `Test`, `MDM`

### Cricsheet Tables (`cricsheet` schema)
| Table | Purpose |
|-------|---------|
| `cricsheet.matches` | Match metadata |
| `cricsheet.match_innings` | Innings summaries (chase targets, super overs) |
| `cricsheet.deliveries` | Ball-by-ball data (DRS reviews, replacements) |
| `cricsheet.players` | Player registry |
| `cricsheet.innings_powerplays` | Powerplay periods |

### Cricinfo Tables (`cricinfo` schema)
| Table | Purpose |
|-------|---------|
| `cricinfo.matches` | Match metadata (Hawkeye source info) |
| `cricinfo.balls` | Ball-by-ball with Hawkeye fields |
| `cricinfo.innings` | Batting scorecards |
| `cricinfo.fixtures` | Schedule/results index |

### Skill Tables (`main` schema, per format: t20, odi, test)
| Table | Key Columns |
|-------|-------------|
| `{format}_player_skill` | batter_scoring_index, batter_survival_rate, bowler_economy_index, bowler_strike_rate |
| `{gender}_{format}_3way_elo` (e.g. `mens_t20_3way_elo`) | batter_run_elo, bowler_run_elo, venue_session_elo, venue_perm_elo — the LIVE tables, 100% coverage all six as of 2026-08-20 (bouncerverse#63). Name declared once by `three_way_elo_table()` in `R/three_way_elo_tables.R`; do not rebuild it inline (two inline declarations drifting is what caused #63). The unprefixed `{format}_3way_elo` names (`t20_3way_elo`, `odi_3way_elo`, `test_3way_elo`) are a **legacy, unpopulated** set — `t20_3way_elo` is empty, `odi_3way_elo`/`test_3way_elo` hold stale women's-only rows — kept only because `database_maintenance.R` still indexes them. |
| `{format}_team_skill` | batting/bowling runs_skill, wicket_skill |
| `{format}_venue_skill` | run_rate, wicket_rate, boundary_rate, dot_rate |
| `{format}_score_projection` | projected_agnostic, projected_full, resource_remaining |
| `team_elo` | Game-level ELO ratings |

## Constants Reference

Constants (`SKILL_ALPHA`, `VENUE_ALPHA`, `THREE_WAY_ELO_START`, `THREE_WAY_RUNS_PER_100_ELO`, expected runs/wicket per ball) vary by format (T20/ODI/Test) — see `R/constants.R`. 3-way ELO attribution weights are additionally gender-specific and differ between run and wicket dimensions — see `get_run_elo_weights()` / `get_wicket_elo_weights()` in `R/constants_3way.R` for current values.

See `data-raw/ARCHITECTURE.md` for complete technical documentation. See `DATA_DICTIONARY.md` for column definitions across all DuckDB tables and computed features.

## Predictions Automation

Predictions run on GHA without the full DuckDB (~22GB and growing — see `../CLAUDE.md`). The pattern follows pannaverse:

1. **Local**: Run heavy pipeline (steps 1-15) → `Rscript data-raw/release/upload_prediction_caches.R` uploads ~50MB of cached aggregates to `predictions-cache` release
2. **GHA**: `predictions-pipeline.yml` downloads caches → loads into temp DuckDB → predicts upcoming fixtures → uploads to `predictions-latest` release
3. **Trigger**: Cricinfo daily scrape dispatches `cricinfo-complete` event to bouncer after new Hawkeye data is uploaded (not the Cricsheet sync - predictions are gated on the Cricinfo Playwright scraper succeeding)

Manual trigger: `gh workflow run predictions-pipeline.yml --repo peteowen1/bouncer --ref dev`
