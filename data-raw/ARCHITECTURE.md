# Bouncer Analytics Architecture

This document explains the complete data flow from raw Cricsheet data through the 5-component prediction and simulation pipeline.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           5-COMPONENT PIPELINE                              │
│                                                                             │
│  1. AGNOSTIC MODEL ──> 2. SKILL INDICES ──> 3. FULL MODEL ──> 4. SIMULATIONS│
│          │                    │                   │                         │
│          └── baseline ────────┴── residuals ──────┴── predictions           │
│                                                          │                  │
│                                              5. PRE-GAME PREDICTIONS        │
│                                                 (Team ELO + Skills)         │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Data Flow

```
STEP 1: install_all_bouncer_data()
           │
STEP 2: Train Agnostic Model ──────────────────────┐
           │                                        │
           ├──> STEP 3: Player Skill Indices ───────┤
           │           (uses agnostic baseline)     │
           ├──> STEP 4: Team Skill Indices ─────────┼──> STEP 7: Full Model
           │           (uses agnostic baseline)     │
           └──> STEP 5: Venue Skill Indices ────────┤
                       (uses agnostic baseline)     │
                                                    │
STEP 6: Team ELO (game-level, unchanged) ───────────┤
                                                    │
                                    ┌───────────────┘
                                    v
                    STEP 8: Pre-Match Features (add team skills)
                                    │
                    STEP 9: Pre-Match Model Training
                                    │
                    STEP 10: Ready for Simulations & Predictions
```

---

## Component 1: Agnostic Delivery Model (Baseline)

**Purpose:** Predict delivery outcomes using ONLY context features (no player/team/venue identity)

**Location:** `data-raw/models/ball-outcome/01_train_agnostic_model.R`

**Features Used:**
| Category | Features |
|----------|----------|
| Match Progress | over, ball, wickets_fallen |
| Score Pressure | runs_difference |
| Time Pressure | overs_left (shortform only) |
| Phase | powerplay/middle/death (shortform) or new_ball/middle/old_ball (Test) |
| Context | innings (1 or 2), format, gender |
| Competition | is_knockout, event_tier |

**EXCLUDES:** player identity, team identity, venue identity

**Output:**
- 7-class probabilities: P(wicket), P(0), P(1), P(2), P(3), P(4), P(6)
- Files: `agnostic_outcome_{format}.ubj`

**Why Important:**
- Provides baseline expectation for residual-based skill indices
- `residual = actual - agnostic_expected`
- Skill indices update based on this residual

---

## Component 2: Residual-Based Skill Indices

All skill indices update per-delivery using EMA on residuals:
```
residual = actual - agnostic_expected
new_skill = (1 - alpha) * old_skill + alpha * residual
```

This converges to the entity's true deviation from expected (bounded and stable).
Positive = performs better than context-expected, Negative = worse.

### Player Skill Index

**Location:** `data-raw/ratings/player/03_calculate_skill_indices.R`

**Output Tables:** `t20_player_skill`, `odi_player_skill`, `test_player_skill`

| Column | Description | Type | Start Value |
|--------|-------------|------|-------------|
| `batter_scoring_index` | Runs deviation from expected | Residual EMA | 0 |
| `batter_survival_rate` | Survival probability | Absolute EMA | ~0.95 |
| `bowler_economy_index` | Runs conceded deviation | Residual EMA | 0 |
| `bowler_strike_rate` | Wicket probability | Absolute EMA | ~0.05 |

**Formula:** `new = (1 - alpha) * old + alpha * observation`
- Indices use residual as observation (deviation from expected)
- Rates use raw outcome as observation (0/1 for survival/wicket)

---

### Team Skill Index (NEW)

**Location:** `data-raw/ratings/team/02_calculate_team_skill_indices.R`

**Output Tables:** `t20_team_skill`, `odi_team_skill`, `test_team_skill`

| Column | Description |
|--------|-------------|
| `batting_team_runs_skill` | Runs deviation when batting |
| `batting_team_wicket_skill` | Wicket rate deviation when batting |
| `bowling_team_runs_skill` | Runs deviation when bowling |
| `bowling_team_wicket_skill` | Wicket rate deviation when bowling |

**Alpha:** 50% of player alpha (teams change composition slowly)

---

### Venue Skill Index

**Location:** `data-raw/ratings/venue/01_calculate_venue_skill_indices.R`

**Output Tables:** `t20_venue_skill`, `odi_venue_skill`, `test_venue_skill`

| Column | Type | Description |
|--------|------|-------------|
| `venue_run_rate` | Residual-based | Runs deviation from expected |
| `venue_wicket_rate` | Residual-based | Wicket deviation from expected |
| `venue_boundary_rate` | Raw EMA | Boundary probability (no baseline) |
| `venue_dot_rate` | Raw EMA | Dot ball probability (no baseline) |

**Alpha:** Lower than players (venues change even slower)

---

### Team ELO (Game-Level)

**Location:** `data-raw/ratings/team/01_calculate_team_elos.R`

**Output Table:** `team_elo`

| Column | Description |
|--------|-------------|
| `elo_result` | Result-based ELO rating |
| `elo_roster_combined` | Roster-aggregated ELO (from player ratings) |
| `matches_played` | Experience indicator |

**Note:** Kept unchanged - works well for match-level predictions

---

## Component 3: Full Delivery Model

**Purpose:** Maximum prediction accuracy using ALL features

**Location:** `data-raw/models/ball-outcome/02_train_full_model.R`

**Features Used:**
| Category | Features |
|----------|----------|
| Context | All agnostic features |
| Player Skills | batter_scoring_index, batter_survival_rate, bowler_economy_index, bowler_strike_rate |
| Team Skills | batting_team_runs_skill, batting_team_wicket_skill, bowling_team_runs_skill, bowling_team_wicket_skill |
| Venue Skills | venue_run_rate, venue_wicket_rate, venue_boundary_rate, venue_dot_rate |

**Output:**
- 7-class probabilities (same as agnostic)
- Files: `full_outcome_{format}.ubj`

**Usage:** Match simulation, per-delivery predictions

---

## Component 4: Simulations

**Location:** `R/delivery_simulation.R`

### Ball-by-Ball Match Simulation

```r
# Quick simulation with default skills
model <- load_full_model("t20")
result <- quick_match_simulation(model, format = "t20")

# Full control simulation
result <- simulate_match_ballbyball(
  model, format = "t20",
  team1_batters, team1_bowlers,
  team2_batters, team2_bowlers,
  team1_skills, team2_skills,
  venue_skills,
  mode = "categorical"  # or "expected"
)
```

**Modes:**
- `categorical`: Draw from P(wicket), P(0), P(1), ..., P(6) distribution
- `expected`: Use E[runs] = sum(P(i) * runs(i)) directly (faster)

### Player Attribution (Zero-Ablation)

**Location:** `R/player_attribution.R`

Since the model uses skill VALUES (not player identity), traditional SHAP doesn't directly give player attribution.

**Solution: Zero-Ablation Comparison**
```r
# Full prediction (with player skills)
full_pred <- predict_full_model(model, features)

# Ablated prediction (set player skills to 0/baseline)
features$batter_scoring_index <- 0
features$batter_survival_rate <- 0
ablated_pred <- predict_full_model(model, features)

# Player contribution = difference
batter_contribution <- full_pred - ablated_pred
```

---

## Component 5: Pre-Game Predictions

**Location:** `data-raw/models/pre-match/`

**Features (including new skills):**

| Category | Features |
|----------|----------|
| Team ELO | team1_elo_result, team2_elo_result, elo_diff |
| Roster ELO | team1_roster_elo, team2_roster_elo |
| Player Skills | Aggregated batting/bowling indices per team |
| Team Skills (NEW) | team_runs_skill_diff, team_wicket_skill_diff |
| Venue Skills (NEW) | venue_run_skill, venue_wicket_skill, venue_boundary, venue_dot |
| Form | Last 5 match results |
| Head-to-Head | Historical win rate |
| Toss | Winner, decision (bat/bowl) |

**Output:** Win probability for each team

---

## Database Tables

### Core Tables
- `matches` - Match metadata
- `deliveries` - Ball-by-ball data
- `players` - Player registry
- `match_innings` - Innings summaries

### Rating Tables
| Table | Description |
|-------|-------------|
| `t20_player_skill` | Player skill indices (residual-based) |
| `t20_team_skill` | Team skill indices (residual-based) |
| `t20_venue_skill` | Venue skill indices |
| `team_elo` | Game-level team ELO |

### Model Output Tables
- `pre_match_features` - Features for prediction
- `pre_match_predictions` - Model predictions
- `simulation_results` - Monte Carlo outputs

---

## File Locations

### Model Outputs
```
bouncerdata/models/
├── agnostic_outcome_t20.ubj      # Agnostic baseline (T20)
├── agnostic_outcome_odi.ubj      # Agnostic baseline (ODI)
├── agnostic_outcome_test.ubj     # Agnostic baseline (Test)
├── full_outcome_t20.ubj          # Full model (T20)
├── full_outcome_odi.ubj          # Full model (ODI)
├── full_outcome_test.ubj         # Full model (Test)
├── t20_prediction_model.ubj      # Pre-match model
└── *_prediction_features.rds     # Feature data
```

---

## Quick Reference

### Run Full Pipeline
```r
# From bouncer/ directory
source("data-raw/run_full_pipeline.R")
```

### Individual Steps
```r
# Step 2: Agnostic model
source("data-raw/models/ball-outcome/01_train_agnostic_model.R")

# Step 3: Player skills
source("data-raw/ratings/player/03_calculate_skill_indices.R")

# Step 4: Team skills
source("data-raw/ratings/team/02_calculate_team_skill_indices.R")

# Step 5: Venue skills
source("data-raw/ratings/venue/01_calculate_venue_skill_indices.R")

# Step 6: Team ELO
source("data-raw/ratings/team/01_calculate_team_elos.R")

# Step 7: Full model
source("data-raw/models/ball-outcome/02_train_full_model.R")

# Step 8-9: Pre-match
source("data-raw/models/pre-match/02_calculate_pre_match_features.R")
source("data-raw/models/pre-match/03_train_prediction_model.R")
```

### Usage Examples
```r
# Load models
agnostic_model <- load_agnostic_model("t20")
full_model <- load_full_model("t20")

# Simulate match
result <- quick_match_simulation(full_model, "t20")

# Calculate attribution
contributions <- calculate_player_attribution(full_model, deliveries, "t20")

# Get predictions
conn <- get_db_connection()
features <- get_pre_match_features(match_id = "12345", conn = conn)
```

---

## Dependency Matrix

| Component | agnostic_model | player_skill | team_skill | venue_skill | team_elo |
|-----------|----------------|--------------|------------|-------------|----------|
| Player Skill | REQUIRED | - | - | - | - |
| Team Skill | REQUIRED | - | - | - | - |
| Venue Skill | REQUIRED | - | - | - | - |
| Full Model | - | REQUIRED | REQUIRED | REQUIRED | - |
| Pre-Match | - | REQUIRED | Optional | Optional | REQUIRED |
| Simulation | Full Model | REQUIRED | REQUIRED | REQUIRED | - |
| Attribution | Full Model | REQUIRED | Optional | Optional | - |

---

## Key Technical Decisions

### 1. Agnostic Model: XGBoost (not BAM)
- Consistency with full model
- SHAP compatibility
- Faster inference for rating calculations

### 2. Skill Index Update Formula
```r
# Residual-based EMA (correct formula):
residual = actual - agnostic_expected
new_index = (1 - alpha) * old_index + alpha * residual

# This converges to the entity's true deviation from expected
# Unlike old_index + alpha * residual which accumulates unboundedly
```

### 3. Player Attribution via Zero-Ablation
SHAP won't work directly since the model uses skill VALUES, not player IDENTITY. Zero-ablation gives interpretable player contributions.
