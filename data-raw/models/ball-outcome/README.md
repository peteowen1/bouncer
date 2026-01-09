# Ball-Outcome Models

This directory contains models for predicting delivery outcomes in cricket - a 7-class classification problem predicting whether each ball results in a wicket, 0, 1, 2, 3, 4, or 6 runs.

## Overview

Ball-outcome models are the foundation for:
- **In-match win probability** - projected scores use outcome predictions
- **Expected runs/wickets** - aggregate outcome probabilities
- **WPA (Win Probability Added)** - measure player impact per delivery
- **Model-Hybrid ELO** - context-aware player ratings

## Model Architecture

### Format Types

| Format | Match Types | Key Differences |
|--------|-------------|-----------------|
| **Shortform** | T20, ODI | Phase = powerplay/middle/death, overs_left feature |
| **Longform** | Test | Phase = new_ball/middle/old_ball, no overs_left |

T20 and ODI share the shortform model because limited-overs dynamics are similar.

### Model Types

| Model | Strengths | Best For |
|-------|-----------|----------|
| **XGBoost** | Higher accuracy, faster inference, explicit feature importance | Production, predictions |
| **BAM** | Better calibrated probabilities, interpretable smooth terms, venue effects | Analysis, understanding |

## Directory Structure

```
ball-outcome/
├── README.md                          # This file
│
├── Runner Scripts
│   ├── run_outcome_models_t20.R       # T20 pipeline (shortform)
│   ├── run_outcome_models_odi.R       # ODI pipeline (shortform)
│   └── run_outcome_models_test.R      # Test pipeline (longform)
│
├── Model Training
│   ├── model_xgb_outcome_shortform.R  # XGBoost T20/ODI
│   ├── model_xgb_outcome_longform.R   # XGBoost Test
│   ├── model_bam_outcome_shortform.R  # BAM T20/ODI
│   └── model_bam_outcome_longform.R   # BAM Test
│
├── Analysis
│   ├── compare_models.R               # Compare BAM vs XGBoost
│   └── visualize_comparison.R         # Visual comparison
│
└── Utilities
    └── add_all_predictions.R          # Add predictions to database
```

## Quick Start

### Train Models

```r
# From bouncer package root directory
devtools::load_all()

# T20 models (default: XGBoost)
source("data-raw/models/ball-outcome/run_outcome_models_t20.R")

# Test models
source("data-raw/models/ball-outcome/run_outcome_models_test.R")
```

### Configuration

Edit runner scripts to configure:

```r
MODEL_TYPE <- "xgboost"      # "xgboost" (default), "bam", or "both"
RUN_COMPARISON <- TRUE       # Compare BAM vs XGBoost
RUN_VISUALIZATION <- TRUE    # Generate plots
ADD_PREDICTIONS <- TRUE      # Add to deliveries table
```

## Output Files

Models are saved to `bouncerdata/models/`:

| File | Description |
|------|-------------|
| `xgb_outcome_shortform.ubj` | XGBoost T20/ODI model |
| `xgb_outcome_longform.ubj` | XGBoost Test model |
| `model_bam_outcome_shortform.rds` | BAM T20/ODI model |
| `model_bam_outcome_longform.rds` | BAM Test model |
| `model_data_splits_shortform.rds` | Train/test data (shortform) |
| `model_data_splits_longform.rds` | Train/test data (longform) |
| `model_xgb_results_*.rds` | Training metrics |

## Model Features

### Shortform (T20/ODI)
- `innings` - 1st or 2nd innings
- `over` - Current over (0-19 for T20, 0-49 for ODI)
- `ball` - Ball within over (1-6)
- `wickets_fallen` - Wickets down (0-9)
- `runs_difference` - Run rate pressure (positive = batting team ahead)
- `overs_left` - Remaining overs
- `phase` - powerplay / middle / death
- `gender` - male / female
- `batter_bsi`, `bowler_bsi` - Player skill indices

### Longform (Test)
- Same as shortform except:
  - No `overs_left` (unlimited length)
  - `phase` = new_ball / middle / old_ball (based on ball age)

## Key Metrics

### Accuracy
- **What:** % of deliveries predicted correctly
- **Higher is better**
- Typical range: 35-45% (7-class is hard!)

### Log Loss
- **What:** Calibration quality of probability predictions
- **Lower is better**
- More important than accuracy for decision-making

### Category-Specific Accuracy
Shows which outcomes each model handles well:
- Both models struggle with rare outcomes (3 runs, wickets)
- XGBoost typically better on boundaries
- BAM typically better on dots/singles

## Model-Hybrid ELO

Ball-outcome models enable **context-aware ELO ratings** by replacing the traditional ELO expected outcome formula:

### Traditional ELO
```r
expected = 1 / (1 + 10^((opponent_elo - player_elo) / 400))
# Only considers ratings
```

### Model-Hybrid ELO
```r
# Get outcome probabilities from model
probs = model.predict(features)  # [P(wicket), P(0), P(1), P(2), P(3), P(4), P(6)]

# Convert to expected outcome score
outcome_scores = c(0.0, 0.2, 0.4, 0.4, 0.4, 0.7, 1.0)
expected = sum(probs * outcome_scores)

# Use in ELO update
new_elo = current_elo + K * (actual - expected)
```

**Benefits:**
- Death over performances properly rewarded/penalized
- Pressure situations create appropriate ELO swings
- Context-aware player comparisons

**Example:** T20 death over, 15 needed from 6 balls, 8 wickets down
- Traditional ELO: Expected = 0.57 (ignores pressure)
- Hybrid ELO: Expected = 0.24 (model knows this is tough)
- Hit a six: Hybrid rewards +24 ELO vs traditional +13 ELO

## Deployment Recommendations

| Scenario | Recommendation |
|----------|----------------|
| Maximum accuracy | XGBoost |
| Calibrated probabilities | BAM |
| Interpretability | BAM |
| Production speed | XGBoost |
| Best of both | Ensemble (average predictions) |

## Dependencies

- **Required:** xgboost, mgcv
- **Optional:** gratia (for BAM feature importance visualization)

```r
install.packages(c("xgboost", "mgcv", "gratia"))
```

## Troubleshooting

**"Model file not found"**
- Run the training script first
- Check you're in the bouncer package root

**"Data splits file not found"**
- BAM creates data splits; XGBoost reuses them
- For longform: run BAM first, then XGBoost

**Memory issues**
- Close other R sessions
- Reduce batch size in training scripts

**Plots not showing**
- Run in RStudio (not command line)
- Ensure ggplot2 is installed
