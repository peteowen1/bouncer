# Player Rating Systems

This directory contains three complementary player rating approaches, each with different strengths.

## Directory Structure

```
player/
├── shared/                           # Common to all rating systems
│   └── 01_calibrate_expected_values.R
├── dual-elo/                         # Original dual ELO system
│   ├── 01_calculate_dual_elos.R
│   ├── 02_apply_elo_normalization.R
│   ├── 03_validate_dual_elos.R
│   └── run_pipeline.R
├── 3way-elo/                         # 3-way ELO (batter + bowler + venue)
│   ├── 00_compute_centrality_snapshots.R
│   ├── 01_calculate_3way_elo.R
│   ├── 01b_calculate_3way_elo_parallel.R
│   ├── 02_load_3way_parquets.R
│   ├── 03_optimize_3way_params.R
│   ├── 04_calculate_anchor_exposure.R
│   └── 05_validate_3way_elo.R
├── skill-indices/                    # Residual-based skill indices
│   ├── 01_calculate_skill_indices.R
│   ├── 02_validate_skill_indices.R
│   └── graph_calibration.R
└── analysis/
    └── team_elo_by_year.R
```

## System Comparison

| System | Strengths | Use Case |
|--------|-----------|----------|
| **Skill Indices** | Simple, fast, interpretable | General skill tracking |
| **Dual ELO** | Standard ELO approach | Matchup predictions |
| **3-Way ELO** | Accounts for venue effects | Detailed predictions |

## 1. Skill Indices

The simplest approach using exponential moving average on residuals from an agnostic baseline:

```r
residual = actual - agnostic_expected
new_skill = (1 - alpha) * old_skill + alpha * residual
```

**Output Table:** `{format}_player_skill`

| Column | Description |
|--------|-------------|
| `batter_scoring_index` | Runs deviation from expected |
| `batter_survival_rate` | Survival probability |
| `bowler_economy_index` | Runs conceded deviation |
| `bowler_strike_rate` | Wicket-taking probability |

## 2. Dual ELO System

Separate ELO ratings for batting and bowling with normalization to handle population drift.

**Pipeline:**
1. Calculate raw dual ELOs
2. Apply normalization
3. Validate against known rankings

## 3. 3-Way ELO System

Most sophisticated system that jointly models batter, bowler, and venue effects:

```r
expected_runs = agnostic_baseline * (1 + (batter_elo + venue_elo - bowler_elo) * runs_per_100_elo)
```

**Key Innovation:** Uses centrality snapshots to adjust for opponent quality, preventing inflated ratings from playing weak opposition.

**Output Tables:**
- `{format}_3way_player_elo` - Player ELO ratings
- `centrality_snapshots` - Network centrality at each snapshot

**Pipeline:**
1. Compute centrality snapshots (opponent quality adjustment)
2. Calculate 3-way ELO ratings
3. Load results from parquet files
4. Optimize parameters
5. Calculate anchor exposure
6. Validate results

## Shared Scripts

### 01_calibrate_expected_values.R
Calculates expected values (runs, wickets) by format used as baseline for all rating systems.

## Running

```r
# Run all player ratings
source("data-raw/ratings/run_all_ratings.R")

# Or individual systems
source("data-raw/ratings/player/skill-indices/01_calculate_skill_indices.R")
source("data-raw/ratings/player/dual-elo/run_pipeline.R")
```
