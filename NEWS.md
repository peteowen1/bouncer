# bouncer 0.4.0

## Player Rating v2

A rebuilt player rating, running off Cricsheet rather than Cricinfo, adjusted
for the opponent faced and the competition played in, and covering men's and
women's T20 and ODI. Decisions D-P16 to D-P29.

* `calculate_player_rating_v2()` rates batters and bowlers on per-ball runs
  above average, net of the opponent (a two-way alternating-ridge fit) and
  divided by a competition difficulty factor. Selected throughout by
  out-of-sample next-match Spearman, never by leaderboard appearance.
* `calculate_player_value_v2()` combines batting and bowling into one
  per-match-played value, built as quality x opportunity so a specialist's
  batting term reads near zero rather than inheriting an average batter's.
* `fit_competition_factors()` derives competition strength from bridge players
  — the only construction that identifies it, since players and competitions
  are not crossed — anchored on a per-bucket reference set and chained outward.
* `fit_two_way_effects()` exposes the crossed batter/bowler fit.
* `build_player_id_map()` / `canonicalise_player_ids()` merge careers split
  across a bare-name id and a registry hash id.
* `store_player_rating_v2()`, `store_player_value_v2()` and
  `load_player_rating_v2()` persist and read the results.

## Win probability and match state

* `build_cricsheet_win_probability()`, `build_cricinfo_win_probability()` and
  `build_cricinfo_test_win_probability()` score win probability from bouncer's
  own models.
* `fit_resource_surface()` replaces the assumption that a wicket is worth
  exactly six balls with a fitted expected-remaining-runs surface.
* `build_cricsheet_raa()` writes per-ball runs above average.
* `calculate_impact()` supersedes `calculate_epr()`, which remains as a
  deprecated alias.

## Bug fixes

* Player careers split across two ids are merged before rating; 3,775 registry
  ids were the player's name rather than a hash, affecting 2,903 players.
* `download_release_asset()` no longer deletes the destination before the
  swap, which on a failed swap destroyed both the old and the new file.
* Rating writes run in a transaction, so a failed insert cannot leave a bucket
  empty.
* Competition-factor chaining no longer counts a bridge player once per
  neighbouring competition, and clamps at each step rather than only at the end.
* Several silent failures in the versebus download path now raise or warn.
* WPA deltas come from each ball's own pre-delivery state, and are credited to
  the batter's own team.

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
