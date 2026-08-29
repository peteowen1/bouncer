# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Git Workflow

- Work on `dev` branch, not directly on `main`
- PR from `dev` → `main` when features are tested and stable

Verse-level docs (reviews, plans, decision log, work queue) live in `../CLAUDE.md`'s vault at `bouncerverse/` — see `../docs/HOME.md`.

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

> ### ⚠️ "3-Way ELO is the primary system feeding the delivery-level models" was false for every model ever trained until 2026-08-20/21
>
> **The defect (bouncerverse#63, #65):** two production readers — `02_train_full_model.R` and `calculate_roster_elo()` — built the ELO table name as
> `paste0(format, "_3way_elo")`. The ratings actually live in gender-keyed tables
> (`mens_t20_3way_elo`, `womens_odi_3way_elo`, ...); the unprefixed name resolves
> to a **legacy** set where `t20_3way_elo` is empty and `odi_3way_elo` /
> `test_3way_elo` hold stale women's-only rows. Both readers coalesce a join miss
> to a neutral 1400, so **all three ELO features were the constant 1400 for
> every row, in every format, in every full model ever trained** — while the
> pipeline printed `0/N have ELO features` as a success line. `calculate_roster_elo()`
> gave every player 1400, so any two rosters scored identically.
>
> **Fixed** in `bouncer` `f800efa` (both readers now call `three_way_elo_table()`,
> declared once) and `968125b` (rebuild writes to staging, promoted only above
> 99% of expected rows — an interrupted rebuild used to leave a table empty,
> which is how `t20_3way_elo` reached zero). Whether 3-Way ELO is worth keeping
> at all past the ball-by-ball simulation is an open call, not a settled
> "primary system" — see the "decide" verdict in `docs/reference/RATING-ARCHITECTURE.md`.
>
> **Retrained on clean inputs (#65, `434c14c`):** 100% ELO feature coverage for
> the first time (3,221,299/3,221,299 T20 rows, was 0%). Matched comparison
> against the agnostic model, identical held-out rows, bootstrapped by match:
> **T20 +1.80%, ODI +1.69%, Test +2.34%** logloss, 2000/2000 bootstrap draws
> favouring the full model in each format. This retires the earlier "near-irreducible,
> 0.0/0.8/0.8%" ceiling (#16) — that figure was itself measured while these same
> ELO features were zero-filled, so it was a symptom of this bug, not a bound.
>
> **But the retrained full model is still DORMANT** (`docs/reference/MODEL-INVENTORY.md`,
> checked 2026-08-22): `load_full_model()`/`predict_full_outcome()` are absent from
> `NAMESPACE`, no pipeline step or workflow reaches them, and they are not
> republished to the `ball-outcome` release. So 3-Way ELO now genuinely feeds a
> full delivery-outcome model with real signal — a reversal of the total defect
> above — but that model does not yet feed anything else in production.

> ### ⚠️ The WPA feeding the ratings is now OURS (D-P6, 2026-08-13) — but it barely matters
>
> Two corrections live here. The first replaced the WPA source. The second is
> the one that changes what you should work on.
>
> **1. Source: `batting_wpa`/`bowling_wpa` now come from bouncer's own models.**
> `build_cricinfo_win_probability()` scores every T20 and ODI delivery into
> `main.bouncer_wp_from_cricinfo`; `player_game_data.R` joins it. The old
> scraped `cricinfo.balls.win_probability` is still selectable via
> `wp_source = "cricinfo"` for comparison. Ours won on evidence — Brier
> **0.1354 vs 0.2208** over 20,326 ODI deliveries where both exist — and on
> coverage, which went from **8.6% → 100%** (ODI) and **42.9% → 100%** (T20)
> among rows where the player actually batted. **Caveat (calibration-check,
> 2026-08-27): the 0.1354 figure is from a 92-match benchmark subsample the
> team's own handover doc calls "favourable" — a genuine temporal holdout
> scores worse, 0.1779.** Relative comparisons (ours vs. scraped) hold up on
> both; only the absolute number needs the caveat attached when quoted. Full
> detail: `../docs/2026-08-13-SESSION-HANDOVER.md` §8.
>
> **2. WPA contributes ~0.009% of the EPR that feeds BOUNCER.**
> `calculate_epr()` computes `bat_value = batting_wpa + batting_era`, adding a
> probability to a run count:
>
> | | WPA sd | ERA sd | corr(bat_value, ERA) | WPA share of variance |
> |---|---|---|---|---|
> | T20 | 0.126 | 13.05 | +0.99995 | 0.0094% |
> | ODI | 0.133 | 25.88 | +0.99999 | 0.0026% |
>
> **EPR is ERA.** Do not expect any WPA improvement to move a rating until this
> is resolved. And do not "fix" it by standardising the two components — that was
> tested and made the anchor checks worse (Root 14/98 → 98/98 in ODI), so ERA is
> genuinely the stronger player-value signal and WPA is not simply mis-scaled.
>
> **Test WPA exists since 2026-08-13** — `build_cricinfo_test_win_probability()`
> scores all 355,962 Test deliveries (quality honestly weak until the #24
> retrain; strong only in innings 4).
>
> **The WPA delta is flipped to the batter's own team's perspective** (2026-08-13,
> bouncerverse#25). Both stored win probabilities are single-perspective numbers,
> and summing raw deltas docked chasing batters for scoring — corr(batting_wpa,
> runs) was **−0.43 in innings 2**. The flip lives in `.wp_source_sql()`; do not
> difference either WP column directly without it.
>
> **SUPERSEDED BY D-P11 (2026-08-14):** the rating engine is now
> `calculate_impact()` — per-match `raa + kappa*wpa` (kappa fitted: 150 T20,
> 272 ODI) through the same decay/shrinkage/exposure aggregation.
> `calculate_epr()` survives only as a deprecated alias. The history below
> explains why the old engine died; the numbers describe THAT engine.
>
> **`calculate_impact()`'s coverage warning is still load-bearing** — do not silence
> it. Matches with no win probability still reach it as `NA`, and
> `.merge_batting_bowling()` no longer launders those into zeros (it did, for
> 13,668 of 15,012 ODI player-match rows, until 2026-08-13).

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
| **Cricinfo** | `cricinfo_data.R` | Cricinfo ingestion, loaders, fixtures |
| **ELO/Skills** | `three_way_elo.R`, `elo_utils.R`, `team_elo.R`, `team_elo_optimization.R`, `team_correlation.R`, `*_skill_index.R`, `skill_indices.R`, `centrality.R`, `centrality_storage.R` | Rating systems |
| **Stat Ratings** | `stat_ratings.R`, `stat_rating_config.R`, `stat_rating_data.R` | Bayesian stat ratings (PSR, economy, SR, etc.) |
| **BOUNCER Value** | `bouncer_rating.R`, `player_stat_value.R` | Composite player value (PSV/BatV/BowlV → EPR → BOUNCER) |
| **Player Data** | `player_game_data.R`, `player_game_data_storage.R`, `player_game_ratings.R`, `player_career_ratings.R`, `player_career_display.R` | Per-game/career rating storage and display |
| **Home Advantage** | `home_advantage.R` | Home-team detection, venue-country mapping |
| **Models** | `agnostic_model.R`, `model_predictions.R`, `score_projection*.R`, `in_match_prediction.R`, `match_predictions.R`, `pre_match_features.R`, `team_predictions.R` | XGBoost models, predictions |
| **Model Validation** | `calibration_audit.R` | `calibration_audit()`/`worst_calibration_buckets()`/`audit_low_information_state()` — per-cut calibration audit and the low-information-state leak check; see `docs/reference/MODEL-VALIDATION-PROTOCOL.md` |
| **Features** | `feature_engineering.R`, `expected_outcomes.R`, `margin_calculation.R`, `win_probability_added.R`, `match_outcomes.R`, `player_attribution.R`, `hawkeye_features.R` | Feature calculation, WPA, outcomes, Hawkeye |
| **Simulation** | `simulation.R` | Ball-by-ball match simulation |
| **User API** | `user_install.R`, `user_api.R`, `player_metrics.R`, `team_metrics.R` | Public-facing functions, stats |
| **Config** | `constants.R`, `constants_3way.R`, `constants_skill.R`, `globals.R`, `bouncer-package.R` | Constants, globals, package docs |
| **Utilities** | `format_utils.R`, `validation_helpers.R`, `pipeline_state.R`, `pipeline_benchmark.R`, `event_tiers.R`, `team_ids.R` | Helpers, validation, pipeline state, benchmarks |
| **Weather** | `weather.R` | Weather data (Open-Meteo API, venue geocoding) |
| **Tuning** | `xgb_tuning.R` | XGBoost hyperparameter tuning utilities |
| **Visualization** | `visualization.R` | ggplot2-based plotting functions |

### tests/testthat/

Covers ratings, models, data pipeline, and API surfaces — around 30 test files. Run `ls tests/testthat/` for the current list; run a single file with `testthat::test_file("tests/testthat/test-<name>.R")`.

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

Predictions run on GHA without the full 18GB DuckDB. The pattern follows pannaverse:

1. **Local**: Run heavy pipeline (steps 1-15) → `Rscript data-raw/release/upload_prediction_caches.R` uploads ~50MB of cached aggregates to `predictions-cache` release
2. **GHA**: `predictions-pipeline.yml` downloads caches → loads into temp DuckDB → predicts upcoming fixtures → uploads to `predictions-latest` release
3. **Trigger**: Cricinfo daily scrape dispatches `cricinfo-complete` event to bouncer after new Hawkeye data is uploaded (not the Cricsheet sync - predictions are gated on the Cricinfo Playwright scraper succeeding)

Manual trigger: `gh workflow run predictions-pipeline.yml --repo peteowen1/bouncer --ref dev`
