#' bouncer: Ball-by-Ball Cricket Analytics
#'
#' An R package for cricket match analysis and prediction using a novel
#' ball-by-ball skill tracking system. Provides player/team ratings, match
#' predictions, score projections, and Monte Carlo simulations.
#'
#' @section Key Features:
#'
#' - **Skill Tracking**: Player, team, and venue skill indices updated every delivery
#' - **Match Prediction**: Pre-match win probability using ELO and skill models
#' - **In-Match Analysis**: Live win probability and score projections
#' - **Match Simulation**: Ball-by-ball Monte Carlo simulation
#'
#' @section Analysis Functions:
#'
#' - [analyze_player()] - Comprehensive player analysis
#' - [analyze_match()] - Match analysis and key moments
#' - [compare_players()] - Head-to-head player comparison
#' - [compare_teams()] - Team strength comparison
#'
#' @section Prediction Functions:
#'
#' - [predict_match()] - Pre-match outcome prediction
#' - [predict_win_probability()] - In-match win probability
#' - [calculate_projected_score()] - Score projection from game state
#'
#' @section Simulation Functions:
#'
#' - [simulate_match_ballbyball()] - Full match simulation
#' - [simulate_innings()] - Single innings simulation
#' - [simulate_season()] - Tournament simulation
#' - [simulate_ipl_playoffs()] - IPL playoff bracket simulation
#'
#' @section Data Functions:
#'
#' - [install_bouncer_data()] - Download and set up cricket data
#' - [query_matches()] - Query match data
#' - [query_deliveries()] - Query ball-by-ball data
#' - [query_batter_stats()], [query_bowler_stats()] - Player statistics
#'
#' @section Lookup Functions:
#'
#' - [get_player()], [search_players()] - Find players
#' - [get_team()], [search_teams()] - Find teams
#' - [get_team_elo()] - Get team ELO ratings
#'
#' @section Visualization:
#'
#' - [plot_win_probability()] - Win probability timeline
#' - [plot_score_progression()] - Score over time
#' - [plot_skill_progression()] - Player skill history
#' - [plot_elo_history()] - Team ELO history
#' - [plot_player_comparison()] - Visual player comparison
#'
#' @section Getting Started:
#'
#' See `vignette("getting-started")` for installation and basic usage.
#'
#' @section Learn More:
#'
#' - `vignette("player-analysis")` - Deep dive into player skills
#' - `vignette("match-analysis")` - Analyzing completed matches
#' - `vignette("predictions")` - Prediction models explained
#' - `vignette("simulation")` - Monte Carlo simulation guide
#'
#' @keywords internal
"_PACKAGE"
