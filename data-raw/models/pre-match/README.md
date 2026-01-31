# Pre-Game Match Prediction

## Overview

This module provides pre-game match prediction for cricket matches using team ELO ratings, player skill indices, and venue statistics. The system supports all formats (T20, ODI, Test) with format-specific models.

## Quick Start

Run scripts in order. Each script has a `FORMAT` configuration variable that can be set to `"t20"`, `"odi"`, or `"test"`:

```r
devtools::load_all()

# 1. Calculate team ELOs (run via full pipeline or separately)
source("data-raw/ratings/team/01_calculate_team_elos.R")

# 2. Generate pre-match features for all matches
source("data-raw/models/pre-match/02_calculate_pre_match_features.R")

# 3. Train the prediction model
source("data-raw/models/pre-match/03_train_prediction_model.R")

# 4. Evaluate model performance
source("data-raw/models/pre-match/04_evaluate_model.R")

# 5. Generate predictions
source("data-raw/models/pre-match/05_generate_predictions.R")

# 6. Visualize predictions
source("data-raw/models/pre-match/06_visualize_predictions.R")

# Or run the entire pipeline at once:
source("data-raw/models/pre-match/run_pre_match_pipeline.R")
```

## Features (31 total)

### ELO Features
| Feature | Description |
|---------|-------------|
| `elo_diff_result` | Difference in result-based ELO |
| `elo_diff_roster` | Difference in roster-aggregated ELO |
| `team1/2_elo_scaled` | Individual team ELO (scaled) |
| `match_quality` | Average ELO of both teams |

### Skill Index Features
| Feature | Description |
|---------|-------------|
| `bat_scoring_diff` | Difference in batting scoring index |
| `bat_survival_diff` | Difference in batting survival rate |
| `bowl_economy_diff` | Difference in bowling economy index |
| `bowl_strike_diff` | Difference in bowling strike rate |

### Form & H2H Features
| Feature | Description |
|---------|-------------|
| `form_diff` | Difference in recent form (last 5 matches) |
| `h2h_advantage` | Head-to-head win rate advantage |

### Venue Features
| Feature | Description |
|---------|-------------|
| `venue_avg_score` | Average first innings score at venue |
| `venue_chase_success_rate` | Chase success rate at venue |
| `venue_high_scoring` | Binary flag for high-scoring venues |

### Match Context
| Feature | Description |
|---------|-------------|
| `is_knockout` | Whether it's a playoff/knockout match |
| `team1_won_toss` | Whether team1 won the toss |
| `toss_elect_bat` | Whether toss winner elected to bat |

## Model

- **Algorithm**: XGBoost binary classifier
- **Target**: `team1_wins` (1 if team1 wins, 0 otherwise)
- **Training**: All matches except burn-in (2005-2007) and test year (2025)
- **Cross-validation**: 5-fold with early stopping

## Database Tables

Data is stored in DuckDB:

- `team_elo`: Team ELO ratings over time (with format suffix in team_id)
- `pre_match_features`: Computed features per match
- `pre_match_predictions`: Model predictions and outcomes

## Output Files

Models and data saved to `bouncerdata/models/`:

| File | Description |
|------|-------------|
| `{format}_prediction_features.rds` | Feature data with train/test splits |
| `{format}_prediction_model.ubj` | Trained XGBoost model |
| `{format}_prediction_training.rds` | Training results and metrics |
| `{format}_prediction_evaluation.rds` | Detailed evaluation results |
| `team_elo_history.rds` | Team ELO progression for all formats |

## Dependencies

- Requires: Team ELOs from `data-raw/ratings/team/01_calculate_team_elos.R`
- Requires: Player skill indices from `data-raw/ratings/player/03_calculate_skill_indices.R`
- Requires: bouncer package functions via `devtools::load_all()`
- Required package: xgboost
