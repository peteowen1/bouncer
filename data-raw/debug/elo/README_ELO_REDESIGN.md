# 3-Way ELO System: Diagnosis and Redesign

## Status: IMPLEMENTATION COMPLETE, RECALCULATION PENDING

### Problem Identified
The diagnostic script `diagnose_elo_formula.R` revealed:
- Venue ELO range: **8574 points** (-3151 to +5423) - should be <600
- Player ELO range: ~2000 points - should be <800
- These extreme values cause unrealistic predictions

### Solutions Implemented

#### Solution 1: Bounded ELOs (ACTIVE)
Constants in `R/constants_3way.R`:
```r
THREE_WAY_PLAYER_ELO_MIN <- 1000  # Hard floor
THREE_WAY_PLAYER_ELO_MAX <- 1800  # Hard ceiling
THREE_WAY_VENUE_ELO_MIN <- 1200   # Tighter floor
THREE_WAY_VENUE_ELO_MAX <- 1600   # Tighter ceiling
THREE_WAY_APPLY_BOUNDS <- TRUE    # Enable bounds
```

Functions in `R/three_way_elo.R`:
- `bound_player_elo()` - Clamps player ELO to 1000-1800
- `bound_venue_elo()` - Clamps venue ELO to 1200-1600

#### Solution 2: Reduced Venue K (ACTIVE)
```r
THREE_WAY_VENUE_PERM_K_MULTIPLIER <- 0.1  # 10x reduction
```

#### Solution 3: Logistic Conversion (OPTIONAL)
```r
THREE_WAY_USE_LOGISTIC_CONVERSION <- FALSE  # Enable if needed
THREE_WAY_LOGISTIC_SENSITIVITY <- 200
THREE_WAY_LOGISTIC_MAX_MULTIPLIER <- 1.5
```

### What You Need To Do

**Step 1: Verify current data has old (unbounded) values**
```r
# In RStudio console:
devtools::load_all()
source("data-raw/debug/elo/diagnose_elo_formula.R")
```

**Step 2: Re-run 3-Way ELO Calculation with Bounds**
```r
# In RStudio console (takes 30-60 min for men's T20):
source("data-raw/ratings/player/3way-elo/01_calculate_3way_elo.R")
```

Or for just Men's T20:
```r
# Set at top of 01_calculate_3way_elo.R:
GENDER_FILTER <- "mens"
FORMAT_FILTER <- "t20"
# Then source the script
```

**Step 3: Measure Improvements**
```r
source("data-raw/debug/elo/calibration_analysis.R")
```

### Success Criteria

| Metric | Before | Target |
|--------|--------|--------|
| Venue ELO range | 8574 | <600 |
| Player ELO range | ~2000 | <800 |
| Run MAE skill | ~1% | >3% |
| Wicket log-loss skill | <0% | >0% |

### Diagnostic Scripts

| Script | Purpose |
|--------|---------|
| `diagnose_elo_formula.R` | Analyze ELO-to-prediction mapping |
| `variance_decomposition.R` | Component contribution analysis |
| `calibration_analysis.R` | Prediction quality and skill scores |
| `test_elo_bounds.R` | Test bounding functions |
| `run_elo_redesign_steps.R` | Execute full redesign process |

### If Improvements Are Insufficient

1. Enable logistic conversion:
   ```r
   THREE_WAY_USE_LOGISTIC_CONVERSION <- TRUE
   ```

2. Further reduce venue K:
   ```r
   THREE_WAY_VENUE_PERM_K_MULTIPLIER <- 0.05  # 20x reduction
   ```

3. Consider alternative approaches:
   - Static venue features (pre-compute, don't use ELO)
   - 2-Way ELO (drop venue, focus on batter vs bowler)
   - Use ELOs as features in a learned model

### Technical Notes

**DuckDB Segfault Issue**: When RStudio is running, Rscript may crash with DuckDB. Run all scripts from within RStudio console.

**Why Bounds?**: Cricket ELO differences of 400 points represent a ~90% expected win probability. An 800-point player range means the best batter would beat the worst 99%+ of the time - unrealistic for cricket skill distribution.
