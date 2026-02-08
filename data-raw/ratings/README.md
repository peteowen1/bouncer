# Ratings Module

This directory contains all rating system implementations for players, teams, and venues.

## Directory Structure

```
ratings/
├── player/           # Player rating systems
│   ├── shared/       # Scripts shared across rating systems
│   ├── dual-elo/     # Original dual ELO system
│   ├── 3way-elo/     # 3-way ELO (batter + bowler + venue)
│   ├── skill-indices/# Residual-based skill indices
│   └── analysis/     # Rating analysis scripts
├── team/             # Team ELO and skill indices
├── venue/            # Venue skill indices
└── projection/       # Score projection system
```

## Rating Systems Overview

### 1. Skill Indices (Residual-Based)
**Location:** `player/skill-indices/`

The simplest approach using exponential moving average on residuals:
```r
residual = actual - agnostic_expected
new_skill = (1 - alpha) * old_skill + alpha * residual
```

Metrics: batter_scoring_index, batter_survival_rate, bowler_economy_index, bowler_strike_rate

### 2. Dual ELO System
**Location:** `player/dual-elo/`

Separate ELO ratings for batters and bowlers with normalization.

### 3. 3-Way ELO System
**Location:** `player/3way-elo/`

Advanced system that jointly models batter + bowler + venue effects:
```r
expected_runs = agnostic_baseline * (1 + (batter_elo + venue_elo - bowler_elo) * runs_per_100_elo)
```

Includes:
- Session venue effects (pitch prep, dew)
- Permanent venue effects (ground size)
- Centrality snapshots for opponent quality adjustment

### 4. Team Ratings
**Location:** `team/`

- **Team ELO** - Game-level win/loss based ratings
- **Team Skill Indices** - Delivery-level residual-based skills

### 5. Venue Ratings
**Location:** `venue/`

Venue-specific run rates, wicket rates, and boundary/dot probabilities.

## Running Pipelines

```r
# Full pipeline (recommended)
source("data-raw/run_full_pipeline.R")

# Individual rating systems
source("data-raw/ratings/player/skill-indices/run_pipeline.R")
source("data-raw/ratings/player/dual-elo/run_pipeline.R")
source("data-raw/ratings/team/run_team_elo_pipeline.R")
```

## Key Constraint

**Rating calculations MUST be processed in strict chronological order** - never parallelize across matches. Sort by `match_date -> match_id -> delivery_id`.
