# Bouncer Analytics Architecture

This document explains the complete data flow from raw data acquisition through the 5-component prediction and simulation pipeline to distribution.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│  DATA ACQUISITION (GHA daily, bouncerdata repo)                        │
│                                                                         │
│  Cricsheet ──→ cricsheet release    (21K+ matches, ball-by-ball JSON)  │
│  Fox Sports ─→ foxsports release    (BBL/T20I/ODI/Test via Chrome)     │
│  Cricinfo ──→ cricinfo release      (Hawkeye tracking data)            │
└──────────────────────────────┬──────────────────────────────────────────┘
                               │ download from releases
                               ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  DuckDB (bouncerdata/bouncer.duckdb ~17GB)                             │
│                                                                         │
│  cricsheet.* (5) │ cricinfo.* (4) │ foxsports.* (3) │ main.* (~50)       │
└──────────────────────────────┬──────────────────────────────────────────┘
                               │ local analytics pipeline (~1hr)
                               ▼
```

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
                    STEP 10: Optimize Projection Parameters
                                    │
                    STEP 11: Calculate Per-Delivery Projections
                                    │
                    Ready for Simulations & Predictions
```

---

## Data Acquisition

Three data sources feed the pipeline, all scraped via GitHub Actions workflows in the `bouncerdata` repo.

### Cricsheet (Primary — Ball-by-Ball)

The backbone of the pipeline. Open-source ball-by-ball JSON data for international and domestic cricket.

- **Workflow**: `cricsheet-daily.yml` (7 AM UTC)
- **Method**: Downloads `recently_added_7_json.zip`, parses new matches, merges with existing parquets
- **Release tag**: `cricsheet` — partitioned deliveries (`deliveries_{match_type}_{gender}.parquet`), matches, players, manifest
- **DuckDB tables**: `cricsheet.matches`, `cricsheet.deliveries`, `cricsheet.players`, `cricsheet.match_innings`, `cricsheet.innings_powerplays`
- **Scale**: ~21K matches, ~10.9M deliveries across T20/IT20/ODI/ODM/Test/MDM × male/female
- **Used by**: All 11 pipeline steps. This is the only data source the analytics pipeline consumes directly.

### Fox Sports (Supplementary — Australian Domestic)

Australian-specific match data, especially BBL/WBBL with richer stats.

- **Workflow**: `foxsports-daily.yml` (10 AM UTC)
- **Method**: `chromote` headless Chrome → intercepts Fox Sports API userkey → scrapes match data
- **Release tag**: `foxsports` — combined parquets per format (BBL, WBBL, TEST, T20I, WT20I, ODI, WODI)
- **DuckDB**: Schema exists (`foxsports.*`) but not populated by the analytics pipeline; data lives as standalone parquets

### Cricinfo (Supplementary — Hawkeye Tracking)

ESPN Cricinfo ball-by-ball data with Hawkeye tracking fields (wagon wheel coordinates, pitch maps, shot types).

- **Workflow**: `cricinfo-daily.yml` (12 PM UTC)
- **Method**: Playwright + stealth (non-headless Chrome via Xvfb) → intercepts Hawkeye API responses during scroll
- **Release tag**: `cricinfo` — per-match `{match_id}_{balls|match|innings}.parquet`
- **DuckDB tables**: `cricinfo.matches`, `cricinfo.balls`, `cricinfo.innings`, `cricinfo.fixtures`
- **Scale**: ~44K matches metadata, ~3.7K with ball-by-ball Hawkeye data
- **Unique fields**: wagonX/Y/Zone, pitchLine/Length, shotType, shotControl, win probability

See `bouncerdata/CLAUDE.md` for workflow manual triggers and troubleshooting.

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

**Location:** `data-raw/ratings/player/skill-indices/01_calculate_skill_indices.R`

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

### Team Skill Index

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

### 3-Way ELO (Per-Delivery)

**Location:** `R/three_way_elo.R`, `R/elo_utils.R`

The primary per-delivery rating system. Decomposes each delivery outcome into batter, bowler, and venue contributions using dual session/permanent ratings.

**Output Tables:** `{format}_3way_elo` (e.g., `t20_3way_elo`)

**Formula:**
```r
expected_runs = agnostic_baseline * (1 + (batter_elo + venue_elo - bowler_elo) * runs_per_100_elo)
```

**Attribution Weights** (Men's T20 run ELO shown; varies by format, gender, and dimension):
| Component | Men's T20 | Description |
|-----------|-----------|-------------|
| `W_BATTER` | 0.612 | Batter contribution |
| `W_BOWLER` | 0.311 | Bowler contribution |
| `W_VENUE_SESSION` | 0.062 | Short-term venue (pitch prep, dew) |
| `W_VENUE_PERM` | 0.015 | Long-term venue (ground size, typical pitch) |

Weights are format-gender-specific and differ between run and wicket dimensions. See `get_run_elo_weights()` / `get_wicket_elo_weights()` in `R/constants_3way.R`.

**Key constants (per format, Men's shown):**
| Constant | T20 | ODI | Test |
|----------|-----|-----|------|
| `ELO_START` | 1400 | 1400 | 1400 |
| `RUNS_PER_100_ELO` | 0.0745 | 0.0826 | 0.0932 |

**Relationship to skill indices:** 3-Way ELO provides base ratings; skill indices capture residual deviation from the agnostic model baseline. Both feed into the Full Model.

---

### PageRank/Centrality (Network Quality Adjustment)

**Location:** `R/centrality.R`, `R/centrality_storage.R`

Network-based quality adjustment that detects isolated cluster inflation. Builds a match network graph and uses PageRank to assess opponent quality — a player with a high ELO from only playing weak opponents will have low centrality.

Works together with 3-Way ELO: ELO provides base ratings, PageRank/centrality adjusts for opponent network quality.

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

**Location:** `R/simulation.R`

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

## Score Projection System

**Location:** `R/score_projection.R`, `data-raw/ratings/projection/`

Projects final innings totals from any game state. Used for:
- Margin of Victory calculation (wickets wins → runs equivalent)
- Per-delivery projections stored in database
- Live score predictions

### Formula

```
projected = cs + a * eis * resource_remaining + b * cs * resource_remaining / resource_used

where:
  cs = current score
  eis = expected initial score (format average)
  resource_remaining = (balls/max_balls)^z * (wickets/10)^y
  resource_used = 1 - resource_remaining
```

### Parameter Optimization

**Location:** `data-raw/ratings/projection/01_optimize_projection_params.R`

Optimizes `a`, `b`, `z`, `y` per segment (format × gender × team_type) using grid search + Nelder-Mead.

**Output:** `bouncerdata/models/projection_params_{segment}.rds`

### Per-Delivery Projections

**Location:** `data-raw/ratings/projection/02_calculate_projections.R`

Calculates and stores projected score for every delivery in the database.

**Output Tables:** `t20_score_projection`, `odi_score_projection`, `test_score_projection`

| Column | Description |
|--------|-------------|
| `projected_agnostic` | Score projection using format average EIS |
| `projected_full` | Score projection using team/venue-adjusted EIS |
| `projection_change_agnostic` | Change from previous delivery |
| `resource_remaining` | Resources left (0-1) |

### Unified Margin Calculation

**Location:** `R/margin_calculation.R`

Converts wickets wins to runs-equivalent using projection:

```r
# Result: "Team2 won by 6 wickets with 2 overs to spare"
calculate_unified_margin(
  team1_score = 160,      # Target (team batting first)
  team2_score = 165,      # Score when chase completed
  wickets_remaining = 6,  # Wickets in hand
  overs_remaining = 2.0,  # Overs remaining (cricket notation)
  win_type = "wickets",
  format = "t20"
)  # Returns negative margin (team2 won)
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
| Team Skills | team_runs_skill_diff, team_wicket_skill_diff |
| Venue Skills | venue_run_skill, venue_wicket_skill, venue_boundary, venue_dot |
| Form | Last 5 match results |
| Head-to-Head | Historical win rate |
| Toss | Winner, decision (bat/bowl) |

**Output:** Win probability for each team

---

## Database Tables

Uses DuckDB schemas: `cricsheet.*`, `cricinfo.*`, `main.*`

### Core Tables (`cricsheet` schema)
- `cricsheet.matches` - Match metadata
- `cricsheet.deliveries` - Ball-by-ball data
- `cricsheet.players` - Player registry
- `cricsheet.match_innings` - Innings summaries
- `cricsheet.innings_powerplays` - Powerplay periods

### Rating Tables (`main` schema, per format: t20/odi/test)
| Table | Description |
|-------|-------------|
| `main.{format}_player_skill` | Player skill indices (residual-based) |
| `main.{format}_3way_elo` | 3-way ELO ratings (batter, bowler, venue) |
| `main.{format}_team_skill` | Team skill indices (residual-based) |
| `main.{format}_venue_skill` | Venue skill indices |
| `main.{format}_score_projection` | Per-delivery score projections |
| `main.team_elo` | Game-level team ELO |

---

## File Locations

### Model Outputs
```
bouncerdata/models/
├── agnostic_outcome_{format}.ubj      # Agnostic baseline (t20/odi/test)
├── full_outcome_{format}.ubj          # Full model (t20/odi/test)
├── {format}_prediction_model.ubj      # Pre-match prediction model
├── {format}_margin_model.ubj          # Margin prediction model
├── {format}_prediction_features.rds   # Pre-match feature data
├── {format}_prediction_training.rds   # Training data
├── projection_params_{format}_{gender}_{team_type}.rds  # Projection parameters
└── team_elo_params_{format}_{gender}_{team_type}.rds    # Team ELO parameters
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
source("data-raw/ratings/player/skill-indices/01_calculate_skill_indices.R")

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

# Step 10-11: Score Projection
source("data-raw/ratings/projection/01_optimize_projection_params.R")
source("data-raw/ratings/projection/02_calculate_projections.R")
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

# Score projection - matches scoreboard display exactly
# Scoreboard shows: "India 80/3 (10.0 overs)"
calculate_projected_score(80, 3, 10.0, "t20")

# With named parameters
calculate_projected_score(
  current_score = 80,   # 80 runs
  wickets = 3,          # 3 wickets fallen
  overs = 10.0,         # 10 overs bowled
  format = "t20"
)

# Unified margin - matches result format
# "Won by 6 wickets with 2 overs to spare"
calculate_unified_margin(
  team1_score = 180,
  team2_score = 185,
  wickets_remaining = 6,   # 6 wickets in hand
  overs_remaining = 2.0,   # 2 overs left
  win_type = "wickets",
  format = "t20"
)
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
| Projection | - | - | - | - | - |
| Simulation | Full Model | REQUIRED | REQUIRED | REQUIRED | - |
| Attribution | Full Model | REQUIRED | Optional | Optional | - |

---

## Distribution

After the pipeline completes, skill tables are distributed through two channels:

### GitHub Releases (peteowen1/bouncerdata)

Pipeline outputs are exported from DuckDB to parquet and uploaded via `piggyback`:

| Release Tag | Content | Files |
|-------------|---------|-------|
| `player_rating` | Per-delivery player skill indices | `{format}_player_skill.parquet` × 3 (~530MB) |
| `team_rating` | Per-delivery team skill indices | `{format}_team_skill.parquet` × 3 (~530MB) |
| `venue_rating` | Per-delivery venue skill indices | `{format}_venue_skill.parquet` × 3 (~350MB) |

**Scripts:** `bouncerdata/scripts/export_parquets.R`, `upload_to_release.R`

### Cloudflare R2 (inthegame-data bucket)

The `build-blog-data.yml` workflow aggregates skill data into compact leaderboard parquets for the blog/website:

```
GitHub Releases (player/team/venue skill parquets)
    │ download
    ▼
build_blog_data.R (aggregate: latest rating per entity, min-ball filters)
    │ produces 12 files: {batting,bowling,teams,venues} × {t20,odi,test}
    ▼
wrangler r2 object put → inthegame-data bucket
```

**Trigger:** `gh workflow run build-blog-data.yml --repo peteowen1/bouncerdata --ref dev`

**Secrets required:** `CLOUDFLARE_R2_TOKEN`, `CLOUDFLARE_ACCOUNT_ID`

---

## Pipeline Dependencies

```
Cricsheet data ─→ Step 2 (Agnostic Model)
                      │
                      ├──→ Steps 3,4,5 (Skill Indices) ──→ Step 7 (Full Model)
                      │                                          │
                      └──→ Step 6 (Team ELO) ─────────→ Step 8 (Pre-Match Features)
                                                              │
                                                         Steps 9-11 (Predictions + Projections)
```

Steps 3, 4, 5 can conceptually run in parallel (all depend on Step 2's agnostic baseline). Steps 8+ require all prior steps.

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
