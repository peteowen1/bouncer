# bouncer

Advanced cricket analytics with player ELO rating system.

## Overview

`bouncer` is an R package for cricket analytics featuring a **novel player ELO rating system**. Unlike traditional cricket statistics, bouncer calculates dynamic ELO ratings for batting and bowling at the **ball-by-ball level**, providing unprecedented insights into player performance and matchup predictions.

## Key Innovation: Player ELO Ratings

The bouncer package introduces cricket-specific ELO ratings that:

- **Update after every delivery** (not just after matches)
- **Separate batting and bowling ratings** for each player
- **Multi-dimensional ratings**: Overall, format-specific (Test/ODI/T20), and opposition-adjusted
- **Context-aware predictions**: Account for venue, match phase, score pressure, etc.
- **Expected runs and wicket probabilities** for any batter-bowler matchup

### Why ELO for Cricket?

No other cricket analytics system currently uses ball-by-ball ELO ratings. This approach provides:

1. **Dynamic player strength assessment** that updates in real-time
2. **Fair comparison across eras** using relative performance metrics
3. **Predictive power** for individual matchups and match outcomes
4. **Objective ratings** independent of team success

## Installation

```r
# Install bouncer package
devtools::install("path/to/bouncer")
```

## Quick Start

```r
library(bouncer)

# Step 1: Install cricket data (one-time setup)
install_bouncer_data(
  formats = c("odi", "t20i"),
  leagues = c("ipl")
)

# Step 2: Get player ELO ratings
kohli_elo <- get_player_elo("V Kohli", match_type = "t20")
bumrah_elo <- get_player_elo("J Bumrah", match_type = "t20")

# Step 3: Predict matchup outcomes
matchup <- predict_matchup_outcome(
  batter = "V Kohli",
  bowler = "J Bumrah",
  context = list(
    match_type = "t20",
    over = 18,
    phase = "death"
  )
)
# Returns expected runs, wicket probability, and advantage

# Step 4: Analyze matchups
head_to_head <- analyze_batter_vs_bowler("V Kohli", "J Bumrah")

# Step 5: Predict match outcomes
match_prediction <- predict_match_outcome(
  team1 = "India",
  team2 = "Australia",
  venue = "MCG",
  match_type = "T20I"
)
```

## Core Functions

### Skill & ELO System

- `get_player_elo()` - Retrieve current player ELO ratings
- `get_team_elo()` - Retrieve current team ELO ratings
- `get_batting_elo()` - Get batting-specific ELO
- `get_bowling_elo()` - Get bowling-specific ELO
- `get_elo_at_delivery()` - ELO at any specific ball
- `get_match_elo_progression()` - Track ELO changes during a match
- `plot_skill_progression()` - Visualize skill over time

### Predictions

- `predict_match()` - Pre-match win probability
- `predict_match_outcome()` - Detailed match outcome prediction
- `predict_matchup_outcome()` - Batter vs bowler prediction
- `predict_win_probability()` - In-match win probability
- `calculate_projected_score()` - Project innings total from current state

### Player Analytics

- `player_batting_stats()` - Aggregate batting metrics (one player or all)
- `player_bowling_stats()` - Aggregate bowling metrics (one player or all)
- `analyze_batter_vs_bowler()` - Historical head-to-head analysis
- `rank_players()` - Player rankings by ELO

### Team Analytics

- `team_batting_stats()` - Aggregate team batting metrics (one team or all)
- `team_bowling_stats()` - Aggregate team bowling metrics (one team or all)
- `head_to_head()` - Historical team vs team record
- `compare_teams()` - Team comparison with win probability

### Venue Analytics

- `venue_stats()` - Venue scoring rates and conditions (one venue or all)

## How the ELO System Works

### Ball-by-Ball Updates

After each delivery:

1. **Calculate expected outcome** based on current batter/bowler ELO ratings
2. **Determine actual outcome** (runs scored, wicket taken, etc.)
3. **Update both ratings** using the ELO formula

### Outcome Scoring

For batters (0 to 1 scale):
- **Wicket**: 0.0 (worst)
- **Dot ball**: 0.2
- **1-2 runs**: 0.4
- **4 runs**: 0.7
- **6 runs**: 1.0 (best)

Bowlers get the inverse score (batter 0.8 → bowler 0.2).

### K-Factor (Learning Rate)

Varies by match importance and player experience:
- **Test**: 32
- **ODI**: 24
- **T20**: 20
- **Domestic**: 16

Newer players have higher K-factors (learn faster), which gradually decreases with experience.

### Multi-Layer Ratings

Each player maintains separate ELO ratings for:

1. **Overall rating** (across all matches)
2. **Format-specific** (Test, ODI, T20)
3. **Opposition-adjusted** (vs specific teams)
4. **Venue-specific** (at particular grounds)

## Example: Predicting a Matchup

```r
# Kohli (1650 batting ELO) vs Bumrah (1700 bowling ELO) in T20 death overs

# Expected outcome for Kohli
E_kohli = 1 / (1 + 10^((1700 - 1650) / 400))
# ≈ 0.43 (Bumrah favored)

# Expected runs
expected_runs = 0.43 × 7.5 (T20 avg) × context_adjustments
# ≈ 2.8 runs per ball (accounting for death overs pressure)

# Wicket probability
wicket_prob = 0.025 (T20 base) × (1 - 0.43) × context_adjustments
# ≈ 0.018 or 1.8% chance of wicket this ball
```

## Advantages Over Traditional Stats

| Traditional Stats | Bouncer ELO System |
|-------------------|-------------------|
| Cumulative averages | Dynamic, up-to-date ratings |
| No opponent context | Opposition-adjusted ratings |
| Format-agnostic | Format-specific ratings |
| Team-dependent | Individual player assessment |
| Historical only | Predictive capability |
| Match-level updates | Ball-by-ball updates |

## Data Source

All data comes from [Cricsheet](https://cricsheet.org). The bouncer package includes functions to:

- Download data from Cricsheet
- Parse JSON files
- Store in DuckDB database with Parquet support
- Manage ball-by-ball delivery records
- Track match metadata and player registry

## Package Architecture

```
bouncer/
├── Data Management
│   ├── Download from Cricsheet
│   ├── Parse JSON files
│   ├── Store in DuckDB
│   └── Manage database
│
└── Analytics
    ├── ELO rating system
    ├── Prediction models
    ├── Player analytics
    └── Team analytics

bouncerdata/              # Data storage directory (not a package)
└── bouncer.duckdb        # Cricket database
```

## Development Status

**Phase 1 (Complete)**: Data infrastructure
- ✅ Cricsheet data download and parsing
- ✅ DuckDB database schema
- ✅ Data loading pipeline

**Phase 2 (Complete)**: Core Skill System
- ✅ Residual-based skill indices
- ✅ Ball-by-ball skill tracking
- ✅ Team and venue skill indices
- ✅ Basic predictions

**Phase 3 (Complete)**: Advanced Features
- ✅ Format-specific skills (T20, ODI, Test)
- ✅ Context-aware score projection
- ✅ Player analytics (`compare_players()`, `analyze_player()`)
- ✅ Match simulation (ball-by-ball)
- ✅ Season simulation
- ✅ Visualization functions

## Contributing

This is a novel approach to cricket analytics. Contributions, suggestions, and feedback are welcome!

## License

MIT

## Acknowledgments

- Data: [Cricsheet](https://cricsheet.org)
- Inspired by ELO rating systems in chess and other sports
- Built with: DuckDB, dplyr, DBI

## Citation

If you use bouncer in research or publications, please cite:

```
@software{bouncer2026,
  title = {bouncer: Cricket Analytics with Player ELO Rating System},
  author = {Pete Owen},
  year = {2026},
  url = {https://github.com/peteowen1/bouncerverse}
}
```
