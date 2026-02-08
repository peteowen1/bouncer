# Debug Scripts

Exploratory and debugging scripts organized by topic. These are **NOT** part of the main pipeline but are useful for investigating issues and validating calculations.

## Guidelines

### When to Create Debug Scripts
- Investigating unexpected behavior or bugs
- Validating calculation correctness
- Exploratory data analysis for new features
- One-time queries for specific questions

### When to Archive Scripts
Move scripts to `archive/` when:
- The issue has been resolved and verified
- The script was for a one-time investigation
- The functionality has been incorporated into the main package
- The script is no longer relevant (e.g., references deprecated features)

### When to Move to Validation
If a debug script becomes a routine check (run regularly to ensure data quality), move it to `data-raw/validation/` instead.

### Naming Conventions
- `check_*.R` - Quick verification scripts
- `debug_*.R` - In-depth debugging sessions
- `investigate_*.R` - Issue-specific investigations
- `test_*.R` - Testing specific functionality (not testthat tests)
- `explore_*.R` - Exploratory analysis

## Directory Structure

```
debug/
├── elo/              # ELO rating system debugging
├── pagerank/         # PageRank and network analysis
├── opponent-quality/ # Opponent quality calculations
├── centrality/       # Network centrality metrics
├── pipeline/         # Full pipeline testing
└── archive/          # One-time verification scripts (completed)
```

## Usage

All debug scripts follow this template:

```r
library(DBI)
devtools::load_all()

conn <- get_db_connection(read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

# Debug code here
```

## Categories

### elo/
Scripts for debugging ELO calculations and rankings:
- `check_3way_elo_results.R` - Verify 3-way ELO output
- `check_elo_tables.R` - Verify ELO table structure and values
- `check_known_players.R` - Check ratings for known star players
- `debug_3way_elo_rankings.R` - Investigate 3-way ELO rankings
- `league_elo_analysis.R` - Analyze ELO by league/event
- `player_elo_leaderboards.R` - Generate ELO leaderboards
- `query_top_players.R` - Query top-rated players
- `show_elo_rankings.R` - Display current ELO leaderboards
- `test_calibration.R` - Test ELO calibration
- `test_league_baseline.R` - Test league-specific baselines

### pagerank/
Network-based rating analysis:
- `debug_pagerank_analysis.R` - Core PageRank debugging
- `investigate_pagerank.R` - Investigate specific PageRank issues
- `pagerank_distribution.R` - Analyze PageRank value distributions
- `pagerank_percentile_analysis.R` - Percentile-based analysis
- `top_batters_with_pagerank.R` - Top batter rankings
- `check_cluster_bowlers.R` - Detect isolated cluster inflation

### opponent-quality/
Opponent quality penalty calculations:
- `debug_opponent_quality.R` - Core opponent quality debugging
- `test_opponent_quality_penalty.R` - Test penalty calculations
- `test_unique_opponents.R` - Verify unique opponent counting

### centrality/
Network centrality metrics:
- `check_centrality_*.R` - Various centrality verification scripts
- `explore_league_centrality.R` - League-level centrality analysis
- `test_combined_centrality.R` - Combined centrality measures
- `test_component_normalization.R` - Component normalization testing
- `test_neighbor_degree.R` - Neighbor degree calculations

### pipeline/
Full pipeline testing:
- `check_t20_balls.R` - Verify T20 ball counts
- `check_t20_count.R` - Verify T20 match counts
- `test_full_pipeline.R` - End-to-end pipeline verification
- `test_new_functions.R` - Test newly added functions

### archive/
One-time verification scripts that are no longer needed for active debugging.
Scripts here have completed their purpose but are preserved for reference.

**Player investigations (completed):**
- `babar_investigation.R` - Babar Azam rating investigation
- `check_pandya.R`, `check_pandya2.R` - Hardik Pandya rating investigation
- `td_andrews_investigation.R` - T.D. Andrews rating investigation

**Infrastructure checks (verified):**
- `check_db.R`, `check_db_path.R` - Database connectivity checks
- `verify_final_fix.R` - PageRank opponent diversity fix (verified Jan 2025)
- `verify_gender_mixing.R` - Gender network isolation check (verified Jan 2025)
- `post_update_analysis.R` - Post-pipeline update analysis
