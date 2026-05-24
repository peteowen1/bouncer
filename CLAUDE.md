# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Git Workflow

- Work on `dev` branch, not directly on `main`
- PR from `dev` → `main` when features are tested and stable

## Development Commands

```r
devtools::load_all()    # Load for development
devtools::document()    # Generate docs (run after changing roxygen2 comments)
devtools::check()       # Full package check
devtools::test()        # Run all tests
testthat::test_file("tests/testthat/test-elo-core.R")  # Single test file
```

## Package Overview

Cricket analytics R package with **ball-by-ball player ratings** and a **15-step prediction pipeline**:

```
Steps 1-11: AGNOSTIC → SKILLS → FULL MODEL → PRE-MATCH → PROJECTIONS
Steps 12-15: IN-MATCH MODELS → PLAYER GAME DATA → STAT RATINGS → CAREER RATINGS (BOUNCER)
```

### Rating Systems (3 complementary approaches)

| System | Files | Use Case |
|--------|-------|----------|
| **3-Way ELO** | `three_way_elo.R` | Primary system: Batter + Bowler + Venue (dual session/permanent) |
| **PageRank/Centrality** | `centrality.R`, `centrality_storage.R` | Network-based quality adjustment (detects isolated cluster inflation) |
| **Stat Ratings** | `stat_ratings.R`, `stat_rating_config.R` | Bayesian per-game stat ratings (PSR, economy, SR, etc.) |

3-Way ELO + centrality feed the delivery-level models. Stat ratings feed the BOUNCER composite value system (`bouncer_rating.R`). Glicko is deprecated and archived in `data-raw/_deprecated/`.

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
| **Features** | `feature_engineering.R`, `expected_outcomes.R`, `margin_calculation.R`, `win_probability_added.R`, `match_outcomes.R`, `player_attribution.R`, `hawkeye_features.R` | Feature calculation, WPA, outcomes, Hawkeye |
| **Simulation** | `simulation.R` | Ball-by-ball match simulation |
| **User API** | `user_install.R`, `user_api.R`, `player_metrics.R`, `team_metrics.R` | Public-facing functions, stats |
| **Config** | `constants.R`, `constants_3way.R`, `constants_skill.R`, `globals.R`, `bouncer-package.R` | Constants, globals, package docs |
| **Utilities** | `format_utils.R`, `validation_helpers.R`, `pipeline_state.R`, `pipeline_benchmark.R`, `event_tiers.R`, `team_ids.R` | Helpers, validation, pipeline state, benchmarks |
| **Weather** | `weather.R` | Weather data (Open-Meteo API, venue geocoding) |
| **Tuning** | `xgb_tuning.R` | XGBoost hyperparameter tuning utilities |
| **Visualization** | `visualization.R` | ggplot2-based plotting functions |

### tests/testthat/ - Test Files (21 files)

```r
# Core rating tests
testthat::test_file("tests/testthat/test-elo-core.R")        # ELO calculations
testthat::test_file("tests/testthat/test-three-way-elo.R")   # 3-way ELO system
testthat::test_file("tests/testthat/test-centrality.R")      # PageRank/centrality
testthat::test_file("tests/testthat/test-skill-indices.R")   # Skill index calculations
testthat::test_file("tests/testthat/test-skill-utils.R")     # Skill utility functions

# Model & prediction tests
testthat::test_file("tests/testthat/test-agnostic-model.R")  # Agnostic model
testthat::test_file("tests/testthat/test-simulation.R")      # Match simulation
testthat::test_file("tests/testthat/test-score-projection.R")# Score projections
testthat::test_file("tests/testthat/test-team-predictions.R")# Team match predictions
testthat::test_file("tests/testthat/test-stat-functions.R")  # Statistical functions

# Data pipeline & API tests
testthat::test_file("tests/testthat/test-parser.R")          # Cricsheet parsing
testthat::test_file("tests/testthat/test-database.R")        # DB connections/schema
testthat::test_file("tests/testthat/test-database-mock.R")   # DB mock tests
testthat::test_file("tests/testthat/test-data-loaders.R")    # Data loading functions
testthat::test_file("tests/testthat/test-fox-scraper.R")     # Fox Sports scraper
testthat::test_file("tests/testthat/test-cricinfo-data.R")   # Cricinfo data integration
testthat::test_file("tests/testthat/test-pipeline-integration.R") # Full pipeline
testthat::test_file("tests/testthat/test-user-api.R")        # User-facing functions
testthat::test_file("tests/testthat/test-format-utils.R")    # Format utilities
testthat::test_file("tests/testthat/test-validation-helpers.R") # SQL/input validation
testthat::test_file("tests/testthat/test-visualization.R")   # Plotting functions
```

### debug/ - Scratch Scripts (gitignored)

Throwaway scripts for debugging, CRAN prep, one-off checks. Everything in `debug/` is gitignored and excluded from the package tarball. Use this instead of creating temp files at the package root.

```
debug/
├── run_test.R           # Quick throwaway test scripts
├── run_check.R          # devtools::check() runner
└── *.R                  # Any temporary/scratch work
```

### data-raw/ - Analysis Scripts (NOT part of package)

```
data-raw/
├── run_full_pipeline.R   # Main pipeline entry point
├── ARCHITECTURE.md       # Complete technical documentation
├── _deprecated/          # Archived systems (dual ELO, Glicko)
├── data-acquisition/     # Download scripts (Cricsheet, Fox Sports)
├── debug/                # Debug scripts organized by topic
│   ├── elo/              # ELO rating debugging
│   ├── pagerank/         # PageRank/network analysis
│   ├── opponent-quality/ # Opponent quality calculations
│   ├── centrality/       # Network centrality metrics
│   ├── pipeline/         # Full pipeline testing
│   └── archive/          # One-time verification scripts
├── logo/                 # Package logo generation (create_logo.R)
├── ratings/
│   ├── player/           # Player rating systems
│   │   ├── shared/       # Calibration scripts
│   │   ├── dual-elo/     # Original dual ELO system
│   │   ├── 3way-elo/     # 3-way ELO (batter+bowler+venue) [Step 5b]
│   │   ├── skill-indices/# Residual-based skill indices [Step 3]
│   │   ├── stat-ratings/ # Bayesian stat ratings + career ratings [Steps 13-15]
│   │   └── analysis/     # Rating analysis scripts
│   ├── team/             # 01_calculate_team_elos.R, 02_calculate_team_skill_indices.R
│   ├── venue/            # 01_calculate_venue_skill_indices.R
│   └── projection/       # 01_optimize_projection_params.R, 02_calculate_projections.R
├── models/
│   ├── ball-outcome/     # 01_train_agnostic_model.R, 02_train_full_model.R
│   │   └── legacy/       # Deprecated BAM models
│   ├── in-match/         # Projected score + win probability models [Step 12]
│   └── pre-match/        # Pre-game prediction models
├── simulation/           # Match/season simulation scripts
├── release/              # GitHub release upload scripts
├── utils/                # Utility/maintenance scripts
├── validation/           # Data validation scripts
└── archive/              # Deprecated scripts (preserved for reference)
```

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
- Never manually edit NAMESPACE (roxygen2 generates it)
- All exported functions need `@export` AND a roxygen2 title/description (bare `@export` without a title generates a NAMESPACE entry but no man page, which breaks pkgdown)
- Use `@importFrom pkg func` for external functions
- Global variables declared in `globals.R` to avoid R CMD check NOTEs
- ~173 exports, ~70 R files; `_pkgdown.yml` must match NAMESPACE exactly

### Documentation (pkgdown)
- Site: https://peteowen1.github.io/bouncer/ (deployed via GitHub Actions on push to `main`)
- Logo: `man/figures/logo.png` (reproducible via `data-raw/logo/create_logo.R`)
- Every NAMESPACE export must appear in `_pkgdown.yml` reference sections
- After adding/removing exports, verify alignment: compare `export()` lines in NAMESPACE against `_pkgdown.yml` contents
- Quick check: `grep '^export(' NAMESPACE | sed 's/export(//;s/)//' | sort > /tmp/ns.txt && grep '^ *- ' _pkgdown.yml | sed 's/^ *- //' | sort > /tmp/pkgdown.txt && diff /tmp/ns.txt /tmp/pkgdown.txt`
- 6 vignettes in `vignettes/`: getting-started, player-analysis, match-analysis, predictions, simulation, database-schema

### Rating Calculations
- **MUST be processed in strict chronological order** - never parallelize
- Sort by `match_date → match_id → delivery_id`

### Analysis Scripts (`data-raw/`)
- Scripts should be simple, calling package functions
- **Do NOT define functions in analysis scripts** - put reusable code in `R/`
- Use RStudio outline sections: `# Section Title ----`

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
| `{format}_3way_elo` | batter_run_elo, bowler_run_elo, venue_session_elo, venue_perm_elo |
| `{format}_team_skill` | batting/bowling runs_skill, wicket_skill |
| `{format}_venue_skill` | run_rate, wicket_rate, boundary_rate, dot_rate |
| `{format}_score_projection` | projected_agnostic, projected_full, resource_remaining |
| `team_elo` | Game-level ELO ratings |

## Constants Reference (from `R/constants.R`, `R/constants_3way.R`)

| Constant | T20 | ODI | Test |
|----------|-----|-----|------|
| `SKILL_ALPHA` | 0.01 | 0.008 | 0.005 |
| `VENUE_ALPHA` | 0.002 | 0.001 | 0.0005 |
| `THREE_WAY_ELO_START` | 1400 | 1400 | 1400 |
| `THREE_WAY_RUNS_PER_100_ELO` | 0.0745 | 0.0826 | 0.0932 |
| `EXPECTED_RUNS/ball` | 1.138 | 0.782 | 0.518 |
| `EXPECTED_WICKET/ball` | 0.054 | 0.028 | 0.017 |

### 3-Way ELO Attribution Weights (Men's T20 run ELO shown)

Weights are **format-gender-specific** and differ between run and wicket dimensions. See `get_run_elo_weights()` / `get_wicket_elo_weights()` in `R/constants_3way.R`.

```r
THREE_WAY_W_BATTER <- 0.612         # Batter contribution
THREE_WAY_W_BOWLER <- 0.311         # Bowler contribution
THREE_WAY_W_VENUE_SESSION <- 0.062  # Short-term venue (pitch prep, dew)
THREE_WAY_W_VENUE_PERM <- 0.015     # Long-term venue (ground size, typical pitch)
```

See `data-raw/ARCHITECTURE.md` for complete technical documentation. See `DATA_DICTIONARY.md` for column definitions across all DuckDB tables and computed features.

## Predictions Automation

Predictions run on GHA without the full 18GB DuckDB. The pattern follows pannaverse:

1. **Local**: Run heavy pipeline (steps 1-15) → `Rscript data-raw/release/upload_prediction_caches.R` uploads ~50MB of cached aggregates to `predictions-cache` release
2. **GHA**: `predictions-pipeline.yml` downloads caches → loads into temp DuckDB → predicts upcoming fixtures → uploads to `predictions-latest` release
3. **Trigger**: Cricsheet daily sync dispatches `cricsheet-complete` event to bouncer after new matches are added

Manual trigger: `gh workflow run predictions-pipeline.yml --repo peteowen1/bouncer --ref dev`
