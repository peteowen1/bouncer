# bouncer 0.3.0

## Major Changes

* **3-Way ELO system** - New primary rating system with separate ELO ratings for
  batter, bowler, and venue (session + permanent) on every delivery. Attribution
  weights: batter 52%, bowler 22%, venue session 21%, venue permanent 5%.

* **PageRank/centrality quality adjustment** - Network-based system that detects
  inflated ratings from isolated competitions. New functions:
  `get_centrality_as_of()`, `get_pagerank_as_of()`, `get_top_pagerank_players()`,
  `calculate_player_centrality()`, `apply_centrality_correction()`.

* **Score projection system** - Per-delivery projected innings totals using
  optimized resource-based models. `calculate_projected_score()` now supports
  all formats with team/venue-adjusted projections.

* **Season & playoff simulation** - `simulate_season()`, `simulate_season_n()`
  for Monte Carlo tournament simulation, plus `simulate_ipl_playoffs()` for
  IPL-specific playoff brackets.

* **Glicko system deprecated** - Removed in favour of the 3-Way ELO + centrality
  approach. Legacy code archived in `data-raw/_deprecated/`.

## Improvements

* `install_all_bouncer_data()` provides one-step data installation from GitHub
  releases, replacing the multi-step manual process.

* New constant getter functions (`get_skill_alpha()`, `get_run_elo_weights()`,
 `get_venue_k_factors()`, etc.) replace hard-coded values with format-aware
  configuration.

* Remote data loading now supported - load data directly from GitHub releases
  without a local DuckDB database.

## Internal

* Major file reorganization: split monolithic files into domain modules
  (database, ELO, centrality, constants, user API).
* Removed ~10 dead code files (old dual-ELO player processing pipeline).
* Tightened exports: ~14 internal functions un-exported, replaced with
  proper getter APIs.
* SQL injection fixes in database query functions.
* Comprehensive test coverage for ELO, simulation, and user API.

# bouncer 0.1.0

Initial CRAN-ready release of bouncer - Cricket analytics with player skill indices.

## Data Management

* `install_bouncer_data()`, `install_all_bouncer_data()` - Download Cricsheet data and store in DuckDB database
* `install_bouncerdata_from_release()` - Install pre-processed data from GitHub releases
* `update_bouncerdata()` - Incremental data updates
* `connect_to_bouncer()`, `disconnect_bouncer()` - Database connection management
* `load_matches()`, `load_deliveries()`, `load_players()`, `load_innings()` - Load core tables into R

## Skill Index System

Novel residual-based skill tracking system that updates ball-by-ball:

* **Player skills**: `load_player_skill()` - Batting scoring index, survival rate, bowling economy, strike rate
* **Team skills**: `load_team_skill()` - Aggregate batting/bowling ability relative to baseline
* **Venue skills**: `load_venue_skill()` - Ground characteristics (run rate, wicket rate, boundaries)

Skill indices represent deviations from format-specific baselines (T20/ODI/Test), allowing cross-format comparisons.

## Player Analysis

* `get_player()` - Player lookup with current skill indices
* `analyze_player()` - Comprehensive player breakdown (batting, bowling, skill history)
* `compare_players()` - Head-to-head player comparison
* `search_players()` - Find players by partial name
* `rank_players()` - Player rankings by skill index
* `player_batting_stats()`, `player_bowling_stats()` - Aggregated career statistics
* `analyze_batter_vs_bowler()` - Specific matchup analysis

## Team Analysis

* `get_team()` - Team lookup with ELO ratings
* `compare_teams()` - Head-to-head team comparison
* `search_teams()` - Find teams by partial name
* `head_to_head()` - Historical team matchup record
* `team_batting_stats()`, `team_bowling_stats()` - Team performance aggregates

## Venue Analysis

* `venue_stats()` - Venue performance characteristics

## Match Analysis

* `analyze_match()` - Comprehensive match breakdown
* `query_matches()` - Search/filter matches

## ELO Ratings

* `get_team_elo()` - Team ELO ratings (game-level)
* `load_team_elo()` - Historical team ELO data

## Predictions

* `predict_match()` - Pre-match win probability
* `predict_match_outcome()` - Match result prediction with confidence
* `predict_win_probability()` - In-game win probability
* `calculate_projected_score()` - Innings score projection
* `calculate_projection_resource()` - Duckworth-Lewis style resource percentage

## Simulation

* `simulate_match_ballbyball()` - Full match simulation
* `quick_match_simulation()` - Fast match simulation
* `simulate_innings()` - Single innings simulation
* `simulate_delivery()` - Ball outcome simulation
* `create_simulation_config()` - Configure simulation parameters

## Visualization

* `theme_bouncer()` - Custom ggplot2 theme for cricket charts
* `plot_score_progression()` - Innings scoring worm
* `plot_win_probability()` - Win probability over time
* `plot_skill_progression()` - Player skill evolution
* `plot_player_comparison()` - Visual player comparison
* `plot_elo_history()` - ELO rating history
* `plot_team_strength()` - Team strength visualization

## Database Queries

Advanced users can query the database directly:

* `query_deliveries()` - Ball-by-ball data queries
* `query_batter_stats()`, `query_bowler_stats()` - Aggregated statistics
* `query_player_stats()` - Combined player statistics

## Test Cricket Utilities

* `calculate_test_projected_score()` - Test match score projection
* `calculate_test_overs_remaining()` - Remaining overs estimation
* `estimate_test_innings_overs_remaining()` - Innings overs estimation

## Data Parsing

* `parse_cricsheet_json()` - Parse Cricsheet JSON files
