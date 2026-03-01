# Calculate Team ELO Ratings by Category
#
# This script calculates team-level ELO ratings for cricket matches,
# split into 4 categories:
#   - mens_club: Male club/franchise leagues (IPL, BBL, etc.)
#   - mens_international: Male international matches
#   - womens_club: Female club/franchise leagues
#   - womens_international: Female international matches
#
# Each category:
#   - Loads its own optimized parameters (from optimize_elo_params.R)
#   - Calculates ELOs independently (no cross-category correlations)
#   - Saves separate history files per format (t20, odi, test)
#
# Output:
#   - team_elo table populated in DuckDB (all categories)
#   - bouncerdata/models/team_elo_history_{format}_{category}.rds (per category)
#
# Usage: Run analysis/optimize_elo_params.R first, then this script


# 1. Setup ----

library(DBI)
library(dplyr)
if (!("bouncer" %in% loadedNamespaces())) devtools::load_all()

cat("\n")
cli::cli_h1("Team ELO Calculation by Category")
cat("\n")


# 2. Configuration ----

FORCE_FRESH <- FALSE

EVENT_FILTER <- NULL
FORMAT_FILTER <- NULL #"t20"
START_SEASON <- NULL

CATEGORIES <- list(
  mens_club = list(gender = "male", team_type = "club"),
  mens_international = list(gender = "male", team_type = "international"),
  womens_club = list(gender = "female", team_type = "club"),
  womens_international = list(gender = "female", team_type = "international")
)

FORMAT_GROUPS <- list(
  t20 = c("T20", "IT20"),
  odi = c("ODI", "ODM"),
  test = c("Test", "MDM")
)

bouncerdata_root <- find_bouncerdata_dir(create = FALSE)
if (is.null(bouncerdata_root)) {
  stop("Cannot locate bouncerdata/ directory. Run from within the bouncer/ workspace with bouncerdata/ as sibling.")
}
params_dir <- file.path(bouncerdata_root, "models")
output_dir <- file.path(bouncerdata_root, "models")
if (!dir.exists(output_dir)) {
  dir.create(output_dir, recursive = TRUE)
}


# 3. Functions from R/ ----
#
# All helper functions are in the package:
#   - load_category_params(): R/team_elo_optimization.R
#   - get_dynamic_k(): R/team_elo_optimization.R
#   - build_home_lookups(): R/home_advantage.R
#   - detect_home_team(): R/home_advantage.R


# 4. Database Connection ----

cli::cli_h2("Connecting to database")
conn <- get_db_connection(read_only = FALSE)
cli::cli_alert_success("Connected to database")


# 5. Load Match Data ----

format_msg <- if (is.null(FORMAT_FILTER)) "ALL" else toupper(FORMAT_FILTER)
cli::cli_h2("Loading {format_msg} matches")

query <- "
  SELECT
    m.match_id,
    m.match_date,
    m.match_type,
    m.gender,
    m.team_type,
    m.season,
    m.event_name,
    m.venue,
    m.team1,
    m.team2,
    m.outcome_winner,
    m.outcome_type,
    m.outcome_by_runs,
    m.outcome_by_wickets,
    m.overs_per_innings,
    m.unified_margin,
    -- Get innings scores for margin calculation (if unified_margin is NULL)
    inn1.total_runs AS team1_score,
    inn1.total_overs AS team1_overs,
    inn2.total_runs AS team2_score,
    inn2.total_overs AS team2_overs
  FROM cricsheet.matches m
  LEFT JOIN cricsheet.match_innings inn1
    ON m.match_id = inn1.match_id AND inn1.innings = 1
  LEFT JOIN cricsheet.match_innings inn2
    ON m.match_id = inn2.match_id AND inn2.innings = 2
  WHERE m.outcome_winner IS NOT NULL
    AND m.outcome_winner != ''
    AND m.gender IS NOT NULL
    AND m.team_type IS NOT NULL
"

params <- list()

if (!is.null(EVENT_FILTER)) {
  query <- paste0(query, " AND m.event_name LIKE ?")
  params <- c(params, paste0("%", EVENT_FILTER, "%"))
}

if (!is.null(FORMAT_FILTER)) {
  match_types <- FORMAT_GROUPS[[FORMAT_FILTER]]
  if (is.null(match_types)) {
    stop("Invalid FORMAT_FILTER: ", FORMAT_FILTER, ". Must be one of: t20, odi, test")
  }
  placeholders <- paste(rep("?", length(match_types)), collapse = ", ")
  query <- paste0(query, " AND m.match_type IN (", placeholders, ")")
  params <- c(params, as.list(match_types))
}

if (!is.null(START_SEASON)) {
  query <- paste0(query, " AND m.season >= ?")
  params <- c(params, START_SEASON)
}

query <- paste0(query, " ORDER BY m.match_date, m.match_id")

if (length(params) > 0) {
  all_matches <- DBI::dbGetQuery(conn, query, params = params)
} else {
  all_matches <- DBI::dbGetQuery(conn, query)
}

cli::cli_alert_success("Loaded {nrow(all_matches)} total {format_msg} matches")

if (nrow(all_matches) == 0) {
  cli::cli_alert_danger("No matches found with current filters")
  stop("No matches to process")
}

cat_summary <- all_matches %>%
  group_by(gender, team_type) %>%
  summarise(n = n(), .groups = "drop")

for (i in seq_len(nrow(cat_summary))) {
  row <- cat_summary[i, ]
  cli::cli_alert_info("{row$gender}_{row$team_type}: {row$n} matches")
}


# 6. Results Storage ----

all_category_results <- list()


# 7. Determine Formats to Process ----

if (is.null(FORMAT_FILTER)) {
  formats_to_process <- names(FORMAT_GROUPS)  # c("t20", "odi", "test")
  cli::cli_alert_info("Processing all formats: {paste(formats_to_process, collapse = ', ')}")
} else {
  formats_to_process <- FORMAT_FILTER
  cli::cli_alert_info("Processing single format: {FORMAT_FILTER}")
}


# 8. Process Each Format and Category ----

for (current_format in formats_to_process) {
  cat("\n")
  cli::cli_h1("Format: {toupper(current_format)}")
  cat("\n")

  # Build format-specific home lookups
  format_home_lookups <- build_home_lookups(all_matches, current_format)

  # Load player ELO HISTORY for date-aware roster calculation ----
  # This loads one row per player per match (not per delivery) to enable

  # looking up player ELOs as of any historical date (no data leakage)
  cli::cli_h2("Loading player ELO history for roster calculation")

  player_elo_table <- paste0(current_format, "_player_elo")
  if (player_elo_table %in% DBI::dbListTables(conn)) {
    cli::cli_alert_info("Loading player ELO history from {player_elo_table}...")

    # Get one ELO per player per match (last delivery of each match)
    # This gives us historical ELOs we can filter by date
    player_elo_history_query <- sprintf("
      WITH batter_per_match AS (
        SELECT
          batter_id as player_id,
          match_id,
          match_date,
          (batter_run_elo_after + batter_wicket_elo_after) / 2.0 as elo,
          ROW_NUMBER() OVER (PARTITION BY batter_id, match_id ORDER BY delivery_id DESC) as rn
        FROM %s
        WHERE batter_run_elo_after IS NOT NULL
      ),
      bowler_per_match AS (
        SELECT
          bowler_id as player_id,
          match_id,
          match_date,
          (bowler_run_elo_after + bowler_wicket_elo_after) / 2.0 as elo,
          ROW_NUMBER() OVER (PARTITION BY bowler_id, match_id ORDER BY delivery_id DESC) as rn
        FROM %s
        WHERE bowler_run_elo_after IS NOT NULL
      )
      SELECT player_id, match_date, elo, 'bat' as role FROM batter_per_match WHERE rn = 1
      UNION ALL
      SELECT player_id, match_date, elo, 'bowl' as role FROM bowler_per_match WHERE rn = 1
    ", player_elo_table, player_elo_table)

    player_elo_history <- data.table::as.data.table(DBI::dbGetQuery(conn, player_elo_history_query))
    player_elo_history[, match_date := as.Date(match_date)]
    data.table::setkey(player_elo_history, player_id, match_date)

    # Split into batting and bowling histories
    batter_elo_history <- player_elo_history[role == "bat", .(player_id, match_date, elo)]
    bowler_elo_history <- player_elo_history[role == "bowl", .(player_id, match_date, elo)]
    data.table::setkey(batter_elo_history, player_id, match_date)
    data.table::setkey(bowler_elo_history, player_id, match_date)

    n_batters <- length(unique(batter_elo_history$player_id))
    n_bowlers <- length(unique(bowler_elo_history$player_id))
    cli::cli_alert_success("Loaded ELO history: {nrow(batter_elo_history)} batter records ({n_batters} players), {nrow(bowler_elo_history)} bowler records ({n_bowlers} players)")

    ROSTER_ELO_ENABLED <- TRUE
  } else {
    cli::cli_alert_warning("Player ELO table {player_elo_table} not found - roster ELO will use default")
    batter_elo_history <- NULL
    bowler_elo_history <- NULL
    ROSTER_ELO_ENABLED <- FALSE
  }

  # Load player participation data for roster inference ----
  if (ROSTER_ELO_ENABLED) {
    cli::cli_alert_info("Loading player participation data...")
    format_match_types <- FORMAT_GROUPS[[current_format]]
    placeholders <- paste(rep("?", length(format_match_types)), collapse = ", ")

    participation_query <- sprintf("
      SELECT DISTINCT
        match_id,
        match_date,
        batting_team,
        bowling_team,
        batter_id,
        bowler_id
      FROM cricsheet.deliveries
      WHERE match_type IN (%s)
    ", placeholders)

    format_participation <- data.table::as.data.table(
      DBI::dbGetQuery(conn, participation_query, params = as.list(format_match_types))
    )
    format_participation[, match_date := as.Date(match_date)]
    cli::cli_alert_success("Loaded participation data: {nrow(format_participation)} records")
  } else {
    format_participation <- NULL
  }

  # Helper function to get player's ELO as of a specific date ----
  get_player_elo_as_of <- function(player_ids, as_of_date, elo_history_dt) {
    if (is.null(elo_history_dt) || length(player_ids) == 0) {
      return(numeric(0))
    }

    # Filter to records before the date for these players
    filtered <- elo_history_dt[player_id %in% player_ids & match_date < as_of_date]

    if (nrow(filtered) == 0) {
      return(numeric(0))
    }

    # Get most recent ELO per player
    latest <- filtered[, .SD[which.max(match_date)], by = player_id]

    return(latest$elo)
  }

  # Helper function to calculate roster ELO for a team (DATE-AWARE) ----
  calc_roster_elo_for_team <- function(team_name, as_of_date, participation_dt,
                                        bat_history_dt, bowl_history_dt, n_recent = 3) {
    if (is.null(participation_dt) || is.null(bat_history_dt)) {
      return(1500)  # Default
    }

    # Find recent matches for this team (before as_of_date)
    team_matches <- participation_dt[
      (batting_team == team_name | bowling_team == team_name) & match_date < as_of_date
    ]

    if (nrow(team_matches) == 0) {
      return(1500)
    }

    # Get most recent n match dates
    match_dates <- sort(unique(team_matches$match_date), decreasing = TRUE)
    recent_dates <- head(match_dates, n_recent)
    recent_matches <- team_matches[match_date %in% recent_dates]

    # Get unique players from recent matches
    batters <- unique(recent_matches[batting_team == team_name, batter_id])
    bowlers <- unique(recent_matches[bowling_team == team_name, bowler_id])

    # Look up ELOs AS OF the match date (no future data!)
    bat_elos <- get_player_elo_as_of(batters, as_of_date, bat_history_dt)
    bowl_elos <- get_player_elo_as_of(bowlers, as_of_date, bowl_history_dt)

    # Aggregate (60% batting, 40% bowling)
    avg_bat <- if (length(bat_elos) >= 3) mean(bat_elos) else 1500
    avg_bowl <- if (length(bowl_elos) >= 3) mean(bowl_elos) else 1500

    combined <- 0.6 * avg_bat + 0.4 * avg_bowl
    return(combined)
  }

  for (category_name in names(CATEGORIES)) {
    cat_config <- CATEGORIES[[category_name]]

    cat("\n")
    cli::cli_h2("Category: {category_name}")
    cat("\n")

    # 8.1 Filter matches by category AND format ----
    format_match_types <- FORMAT_GROUPS[[current_format]]
    matches <- all_matches %>%
      filter(
        gender == cat_config$gender,
        team_type == cat_config$team_type,
        match_type %in% format_match_types
      )

    if (nrow(matches) < 10) {
      cli::cli_alert_warning("Only {nrow(matches)} matches - skipping")
      next
    }

    cli::cli_alert_success("Processing {nrow(matches)} matches")

    # 8.2 Create team IDs and features ----
    matches <- matches %>%
      mutate(
        format = current_format,
        team1_id = make_team_id_vec(team1, gender, format, team_type),
        team2_id = make_team_id_vec(team2, gender, format, team_type),
        outcome_winner_id = make_team_id_vec(outcome_winner, gender, format, team_type)
      ) %>%
      mutate(
        home_team = mapply(detect_home_team, team1, team2, team1_id, team2_id, venue, team_type,
                           MoreArgs = list(club_home_lookup = format_home_lookups$club_home,
                                           venue_country_lookup = format_home_lookups$venue_country))
      )

    n_home <- sum(matches$home_team != 0)
    cli::cli_alert_info("Home matches detected: {n_home}/{nrow(matches)} ({round(n_home/nrow(matches)*100, 1)}%)")

    # 8.2b Calculate unified margin for each match ----
    cli::cli_alert_info("Calculating unified margins...")

    matches <- matches %>%
      mutate(
        # Determine win type from outcome columns
        win_type = case_when(
          !is.na(outcome_by_runs) & outcome_by_runs > 0 ~ "runs",
          !is.na(outcome_by_wickets) & outcome_by_wickets > 0 ~ "wickets",
          outcome_type %in% c("tie", "draw", "no result") ~ outcome_type,
          TRUE ~ "unknown"
        ),
        # Calculate overs remaining for wickets wins (cricket notation)
        overs_remaining = case_when(
          win_type == "wickets" & !is.na(overs_per_innings) & !is.na(team2_overs) ~
            pmax(0, overs_per_innings - team2_overs),
          TRUE ~ 0
        ),
        # Calculate unified margin (use stored if available, else calculate)
        calc_unified_margin = mapply(
          function(t1_score, t2_score, wickets_rem, overs_rem, win_t, fmt, stored_margin) {
            # Use stored margin if available
            if (!is.na(stored_margin)) return(stored_margin)
            # Skip if missing data
            if (is.na(t1_score) || is.na(t2_score)) return(NA_real_)
            if (win_t == "unknown") return(NA_real_)
            # Calculate using package function
            calculate_unified_margin(
              team1_score = t1_score,
              team2_score = t2_score,
              wickets_remaining = wickets_rem,
              overs_remaining = overs_rem,
              win_type = win_t,
              format = fmt
            )
          },
          team1_score, team2_score,
          ifelse(is.na(outcome_by_wickets), 0L, outcome_by_wickets),
          overs_remaining, win_type, format, unified_margin
        )
      )

    n_with_margin <- sum(!is.na(matches$calc_unified_margin))
    margin_mean <- mean(abs(matches$calc_unified_margin), na.rm = TRUE)
    cli::cli_alert_success("Calculated margins for {n_with_margin}/{nrow(matches)} matches (mean abs margin: {round(margin_mean, 1)} runs)")

    # 8.3 Load parameters ----
    cli::cli_h3("Loading parameters")
    cat_params <- load_category_params(category_name, params_dir, current_format)

  # K parameters
  K_MAX <- cat_params$K_MAX
  K_MIN <- cat_params$K_MIN
  K_HALFLIFE <- cat_params$K_HALFLIFE
  OPP_K_BOOST <- cat_params$OPP_K_BOOST
  OPP_K_REDUCE <- cat_params$OPP_K_REDUCE
  HOME_ADVANTAGE <- cat_params$HOME_ADVANTAGE

  # Cross-tier boost (pass directly to function, don't use global)
  CROSS_TIER_K_BOOST_CFG <- cat_params$CROSS_TIER_K_BOOST %||% 0.5
  cli::cli_alert_info("Cross-tier K boost: {CROSS_TIER_K_BOOST_CFG} per tier difference")

  # MOV parameters (from optimization or defaults)
  MOV_EXPONENT_CFG <- cat_params$MOV_EXPONENT %||% 0.7
  MOV_BASE_OFFSET_CFG <- cat_params$MOV_BASE_OFFSET %||% 5
  MOV_DENOM_BASE_CFG <- cat_params$MOV_DENOM_BASE %||% 10
  MOV_ELO_FACTOR_CFG <- cat_params$MOV_ELO_FACTOR %||% 0.004
  MOV_MIN_CFG <- cat_params$MOV_MIN %||% 0.5
  MOV_MAX_CFG <- cat_params$MOV_MAX %||% 2.5
  cli::cli_alert_info("MOV: exp={MOV_EXPONENT_CFG}, max={MOV_MAX_CFG}")

  # Propagation settings
  PROPAGATION_ENABLED <- cat_params$PROPAGATION_ENABLED
  PROPAGATION_FACTOR_CFG <- cat_params$PROPAGATION_FACTOR
  CORR_THRESHOLD_CFG <- cat_params$CORR_THRESHOLD
  CORR_W_OPPONENTS <- cat_params$CORR_W_OPPONENTS
  CORR_W_EVENTS <- cat_params$CORR_W_EVENTS
  CORR_W_DIRECT <- cat_params$CORR_W_DIRECT

  # 8.4 Initialize team ELOs ----
  cli::cli_h2("Initializing team ELOs")

  cli::cli_alert_info("Dynamic K: {K_MAX} (new) -> {K_MIN} (established), halflife = {K_HALFLIFE}")

  # Season regression (optional, disabled by default)
  # Set to < 1.0 to regress ELOs toward baseline each season (e.g., 0.85 = 15% regression)
  SEASON_REGRESSION <- cat_params$SEASON_REGRESSION %||% 1.0
  BASELINE_ELO <- 1500
  STARTING_ELO <- 1300  # New teams start below average and must earn their way up
  if (SEASON_REGRESSION < 1.0) {
    cli::cli_alert_info("Season regression: {SEASON_REGRESSION} (pulls {round((1-SEASON_REGRESSION)*100)}% toward baseline each season)")
  }

  all_teams_eventual <- unique(c(matches$team1_id, matches$team2_id))
  cli::cli_alert_info("Will see {length(all_teams_eventual)} unique teams")
  cli::cli_alert_info("New teams start at {STARTING_ELO} ELO (below 1500 average)")

  # Teams are added dynamically as they first appear
  team_result_elos <- c()
  team_matches_played <- c()
  team_tier_ema <- c()
  teams_seen <- character()

  if (PROPAGATION_ENABLED) {
    cli::cli_alert_info("Propagation: factor={PROPAGATION_FACTOR_CFG}, threshold={CORR_THRESHOLD_CFG}")
  }

  team_opponents <- setNames(vector("list", length(all_teams_eventual)), all_teams_eventual)
  team_events_history <- setNames(vector("list", length(all_teams_eventual)), all_teams_eventual)
  team_matchups <- setNames(vector("list", length(all_teams_eventual)), all_teams_eventual)
  match_results_for_propagation <- list()

  # 8.5 Calculate ELOs match by match ----
  cli::cli_h2("Calculating team ELOs match by match")

  n_matches <- nrow(matches)
  n_teams <- length(all_teams_eventual)
  # Store ALL teams seen so far for EVERY match (for normalization tracking)
  # Pre-allocate max size, will trim later
  n_records <- n_matches * n_teams

  out_team_id <- character(n_records)
  out_match_id <- character(n_records)
  out_match_date <- as.Date(rep(NA, n_records))
  out_format <- character(n_records)
  out_gender <- character(n_records)
  out_team_type <- character(n_records)
  out_elo_before <- numeric(n_records)
  out_elo_after <- numeric(n_records)
  out_elo_diff <- numeric(n_records)
  out_played_in_match <- logical(n_records)
  out_matches_played <- integer(n_records)

  m_team1_id <- matches$team1_id
  m_team2_id <- matches$team2_id
  m_team1 <- matches$team1  # Original team names for roster lookup
  m_team2 <- matches$team2
  m_winner_id <- matches$outcome_winner_id
  m_match_id <- matches$match_id
  m_match_date <- matches$match_date
  m_match_type <- matches$match_type
  m_event_name <- matches$event_name
  m_home_team <- matches$home_team
  m_unified_margin <- matches$calc_unified_margin
  m_season <- matches$season  # For season-based regression

  elo_divisor <- 400
  cli::cli_progress_bar("Processing matches", total = n_matches)
  record_idx <- 1L

  # Track current season for regression
  current_season <- NULL
  season_regressions_applied <- 0L

  for (i in seq_len(n_matches)) {
    # Apply season regression when season changes
    match_season <- m_season[i]
    if (!is.null(match_season) && !is.na(match_season) &&
        (is.null(current_season) || match_season != current_season)) {
      if (!is.null(current_season)) {
        # Regress all team ELOs toward baseline
        team_result_elos <- BASELINE_ELO + SEASON_REGRESSION * (team_result_elos - BASELINE_ELO)
        season_regressions_applied <- season_regressions_applied + 1L
      }
      current_season <- match_season
    }

    team1_id <- m_team1_id[i]
    team2_id <- m_team2_id[i]
    team1_name <- m_team1[i]
    team2_name <- m_team2[i]
    winner_id <- m_winner_id[i]
    match_id <- m_match_id[i]
    match_date <- m_match_date[i]
    match_type <- m_match_type[i]
    event_name <- m_event_name[i]

    # Add new teams dynamically at STARTING_ELO (below average)
    event_tier <- get_event_tier(event_name)
    if (!(team1_id %in% teams_seen)) {
      teams_seen <- c(teams_seen, team1_id)
      team_result_elos[team1_id] <- STARTING_ELO
      team_matches_played[team1_id] <- 0L
      team_tier_ema[team1_id] <- as.numeric(event_tier)
    }
    if (!(team2_id %in% teams_seen)) {
      teams_seen <- c(teams_seen, team2_id)
      team_result_elos[team2_id] <- STARTING_ELO
      team_matches_played[team2_id] <- 0L
      team_tier_ema[team2_id] <- as.numeric(event_tier)
    }

    # Calculate roster ELOs based on player ELOs AS OF this match date (no leakage)
    team1_roster_elo <- calc_roster_elo_for_team(
      team1_name, match_date, format_participation,
      batter_elo_history, bowler_elo_history
    )
    team2_roster_elo <- calc_roster_elo_for_team(
      team2_name, match_date, format_participation,
      batter_elo_history, bowler_elo_history
    )

    team1_elo_before <- team_result_elos[team1_id]
    team2_elo_before <- team_result_elos[team2_id]

    team1_tier <- get_team_tier_rounded(team_tier_ema[team1_id])
    team2_tier <- get_team_tier_rounded(team_tier_ema[team2_id])
    k_multiplier <- get_cross_tier_k_multiplier(team1_tier, team2_tier,
                                                 boost_per_tier = CROSS_TIER_K_BOOST_CFG)

    team1_won <- !is.na(winner_id) && winner_id == team1_id
    team2_won <- !is.na(winner_id) && winner_id == team2_id

    if (team1_won || team2_won) {
      base_k1 <- K_MIN + (K_MAX - K_MIN) * exp(-team_matches_played[team1_id] / K_HALFLIFE)
      base_k2 <- K_MIN + (K_MAX - K_MIN) * exp(-team_matches_played[team2_id] / K_HALFLIFE)

      avg_elo <- 1500
      team1_opp_strength <- (team2_elo_before - avg_elo) / 400
      team2_opp_strength <- (team1_elo_before - avg_elo) / 400

      if (team1_won) {
        k1_modifier <- 1 + max(0, team1_opp_strength * OPP_K_BOOST)
        k2_modifier <- 1 - max(0, team1_opp_strength * OPP_K_REDUCE)
      } else {
        k1_modifier <- 1 - max(0, team2_opp_strength * OPP_K_REDUCE)
        k2_modifier <- 1 + max(0, team2_opp_strength * OPP_K_BOOST)
      }

      k1_modifier <- max(0.5, min(2.0, k1_modifier))
      k2_modifier <- max(0.5, min(2.0, k2_modifier))

      team1_k <- base_k1 * k_multiplier * k1_modifier
      team2_k <- base_k2 * k_multiplier * k2_modifier

      home_adj <- m_home_team[i] * HOME_ADVANTAGE
      exp1 <- 1 / (1 + 10^((team2_elo_before - team1_elo_before - home_adj) / elo_divisor))
      exp2 <- 1 - exp1

      actual1 <- if (team1_won) 1 else 0
      actual2 <- if (team2_won) 1 else 0

      # Calculate margin of victory multiplier (G-factor)
      match_margin <- m_unified_margin[i]
      if (!is.na(match_margin)) {
        winner_elo <- if (team1_won) team1_elo_before else team2_elo_before
        loser_elo <- if (team1_won) team2_elo_before else team1_elo_before
        mov_multiplier <- calculate_mov_multiplier(
          match_margin, winner_elo, loser_elo, current_format,
          mov_exponent = MOV_EXPONENT_CFG,
          mov_base_offset = MOV_BASE_OFFSET_CFG,
          mov_denom_base = MOV_DENOM_BASE_CFG,
          mov_elo_factor = MOV_ELO_FACTOR_CFG,
          mov_min = MOV_MIN_CFG,
          mov_max = MOV_MAX_CFG
        )
      } else {
        mov_multiplier <- 1.0  # No margin data, use standard update
      }

      team1_elo_after <- team1_elo_before + team1_k * mov_multiplier * (actual1 - exp1)
      team2_elo_after <- team2_elo_before + team2_k * mov_multiplier * (actual2 - exp2)
    } else {
      team1_elo_after <- team1_elo_before
      team2_elo_after <- team2_elo_before
    }

    team_result_elos[team1_id] <- team1_elo_after
    team_result_elos[team2_id] <- team2_elo_after
    team_matches_played[team1_id] <- team_matches_played[team1_id] + 1L
    team_matches_played[team2_id] <- team_matches_played[team2_id] + 1L

    team_tier_ema[team1_id] <- update_team_tier(team_tier_ema[team1_id], event_tier)
    team_tier_ema[team2_id] <- update_team_tier(team_tier_ema[team2_id], event_tier)

    team_opponents[[team1_id]] <- c(team_opponents[[team1_id]], team2_id)
    team_opponents[[team2_id]] <- c(team_opponents[[team2_id]], team1_id)
    team_events_history[[team1_id]] <- c(team_events_history[[team1_id]], event_name)
    team_events_history[[team2_id]] <- c(team_events_history[[team2_id]], event_name)

    if (is.null(team_matchups[[team1_id]])) team_matchups[[team1_id]] <- list()
    if (is.null(team_matchups[[team2_id]])) team_matchups[[team2_id]] <- list()
    team_matchups[[team1_id]][[team2_id]] <- (team_matchups[[team1_id]][[team2_id]] %||% 0L) + 1L
    team_matchups[[team2_id]][[team1_id]] <- (team_matchups[[team2_id]][[team1_id]] %||% 0L) + 1L

    # Capture elo_before for ALL teams (before normalization)
    all_elo_before <- team_result_elos

    # Normalize ALL teams to maintain mean of 1500
    current_mean <- mean(team_result_elos)
    drift <- current_mean - 1500
    if (abs(drift) > 0.001) {
      team_result_elos <- team_result_elos - drift
    }

    # Store a row for EVERY team seen so far in this category
    for (team_id in teams_seen) {
      out_team_id[record_idx] <- team_id
      out_match_id[record_idx] <- match_id
      out_match_date[record_idx] <- match_date
      out_format[record_idx] <- current_format
      out_gender[record_idx] <- cat_config$gender
      out_team_type[record_idx] <- cat_config$team_type
      out_elo_before[record_idx] <- all_elo_before[team_id]
      out_elo_after[record_idx] <- team_result_elos[team_id]
      out_elo_diff[record_idx] <- team_result_elos[team_id] - all_elo_before[team_id]
      out_played_in_match[record_idx] <- (team_id == team1_id || team_id == team2_id)
      out_matches_played[record_idx] <- team_matches_played[team_id]
      record_idx <- record_idx + 1L
    }

    if (i %% 1000 == 0) cli::cli_progress_update(set = i)
  }

  cli::cli_progress_done()

  if (season_regressions_applied > 0) {
    cli::cli_alert_success("Applied {season_regressions_applied} season regressions (factor: {SEASON_REGRESSION})")
  }

  # Trim output vectors to actual size (in case we pre-allocated too much)
  actual_records <- record_idx - 1L
  elo_df <- data.frame(
    team_id = out_team_id[1:actual_records],
    match_id = out_match_id[1:actual_records],
    match_date = out_match_date[1:actual_records],
    format = out_format[1:actual_records],
    gender = out_gender[1:actual_records],
    team_type = out_team_type[1:actual_records],
    elo_before = out_elo_before[1:actual_records],
    elo_after = out_elo_after[1:actual_records],
    elo_diff = out_elo_diff[1:actual_records],
    played_in_match = out_played_in_match[1:actual_records],
    matches_played = out_matches_played[1:actual_records],
    stringsAsFactors = FALSE
  )

  cli::cli_alert_success("Calculated ELOs for {nrow(elo_df)} records ({n_matches} matches x {n_teams} teams)")

  # Log normalization stats
  active_records <- elo_df[elo_df$played_in_match, ]
  inactive_records <- elo_df[!elo_df$played_in_match, ]
  cli::cli_alert_info("Active team changes: mean={round(mean(active_records$elo_diff), 2)}, range=[{round(min(active_records$elo_diff), 1)}, {round(max(active_records$elo_diff), 1)}]")
  if (nrow(inactive_records) > 0) {
    cli::cli_alert_info("Normalization adjustments: mean={round(mean(inactive_records$elo_diff), 4)}")
  }

  # 8.7 Store in database ----
  cli::cli_h2("Storing in database")

  existing_tables <- DBI::dbListTables(conn)

  # Check if table needs schema migration (new schema has 'format' column)
  needs_migration <- FALSE
  if ("team_elo" %in% existing_tables) {
    cols <- DBI::dbListFields(conn, "team_elo")
    if (!"format" %in% cols) {
      cli::cli_alert_warning("Old schema detected - dropping and recreating team_elo table")
      DBI::dbExecute(conn, "DROP TABLE team_elo")
      needs_migration <- TRUE
    }
  }

  if (!"team_elo" %in% existing_tables || needs_migration) {
    cli::cli_alert_info("Creating team_elo table with new schema...")
    DBI::dbExecute(conn, "
      CREATE TABLE IF NOT EXISTS team_elo (
        team_id VARCHAR,
        match_id VARCHAR,
        match_date DATE,
        format VARCHAR,
        gender VARCHAR,
        team_type VARCHAR,
        elo_before DOUBLE,
        elo_after DOUBLE,
        elo_diff DOUBLE,
        played_in_match BOOLEAN,
        matches_played INTEGER,
        PRIMARY KEY (team_id, match_id)
      )
    ")
    DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_team_elo_date ON team_elo(team_id, match_date)")
    DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_team_elo_format ON team_elo(format, gender, team_type)")
    DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_team_elo_match ON team_elo(match_id)")
  }

  # Delete existing records for matches we're about to insert
  match_ids <- unique(matches$match_id)
  if (length(match_ids) > 0) {
    batch_size <- 500
    n_batches <- ceiling(length(match_ids) / batch_size)
    for (b in seq_len(n_batches)) {
      start_idx <- (b - 1) * batch_size + 1
      end_idx <- min(b * batch_size, length(match_ids))
      batch_ids <- match_ids[start_idx:end_idx]
      placeholders <- paste(rep("?", length(batch_ids)), collapse = ", ")
      DBI::dbExecute(conn,
        sprintf("DELETE FROM team_elo WHERE match_id IN (%s)", placeholders),
        params = as.list(batch_ids)
      )
    }
  }

  DBI::dbWriteTable(conn, "team_elo", elo_df, append = TRUE)
  cli::cli_alert_success("Stored {nrow(elo_df)} records in team_elo table")

  # 8.8 Save to RDS ----
  cli::cli_h2("Saving ELO history")

  final_elos <- data.frame(
    team_id = names(team_result_elos),
    elo_result = unname(team_result_elos),
    matches_played = unname(team_matches_played),
    primary_tier = sapply(team_tier_ema[names(team_result_elos)], get_team_tier_rounded),
    stringsAsFactors = FALSE
  ) %>%
    arrange(desc(elo_result))

  output_path <- file.path(output_dir, paste0("team_elo_history_", current_format, "_", category_name, ".rds"))

  saveRDS(list(
    category = category_name,
    format = current_format,
    history = elo_df,
    final_standings = final_elos,
    config = list(
      category = category_name,
      format = current_format,
      gender = cat_config$gender,
      team_type = cat_config$team_type,
      k_max = K_MAX,
      k_min = K_MIN,
      k_halflife = K_HALFLIFE,
      opp_k_boost = OPP_K_BOOST,
      opp_k_reduce = OPP_K_REDUCE,
      home_advantage = HOME_ADVANTAGE,
      propagation_enabled = PROPAGATION_ENABLED,
      propagation_factor = PROPAGATION_FACTOR_CFG,
      corr_threshold = CORR_THRESHOLD_CFG,
      n_matches = nrow(matches),
      n_teams = length(teams_seen),
      n_home_matches = n_home,
      created_at = Sys.time()
    )
  ), output_path)

  cli::cli_alert_success("Saved ELO history to {output_path}")

  # 8.9 Show top 10 teams ----
  cli::cli_h3("Top 10 Teams ({toupper(current_format)})")
  top_10 <- head(final_elos, 10)
  for (j in seq_len(nrow(top_10))) {
    team_name <- gsub("_(male|female)_(t20|odi|test)_(club|international)$", "", top_10$team_id[j])
    cli::cli_alert_info("{j}. {team_name}: {round(top_10$elo_result[j])} ({top_10$matches_played[j]} matches)")
  }

  # Store results with format key
  result_key <- paste0(current_format, "_", category_name)
  all_category_results[[result_key]] <- list(
    format = current_format,
    category = category_name,
    n_matches = nrow(matches),
    n_teams = length(teams_seen),
    accuracy = NA,
    top_team = top_10$team_id[1],
    top_elo = round(top_10$elo_result[1])
  )

  } # End category loop
} # End format loop


# 9. Final Summary ----

cat("\n")
cli::cli_h1("Final Summary")
cat("\n")

cat(sprintf("%-10s %-25s %10s %10s %20s %10s\n",
            "Format", "Category", "Matches", "Teams", "Top Team", "Top ELO"))
cat(paste(rep("-", 90), collapse = ""), "\n")

for (result_key in names(all_category_results)) {
  result <- all_category_results[[result_key]]
  top_team_name <- gsub("_(male|female)_(t20|odi|test)_(club|international)$", "", result$top_team)
  cat(sprintf("%-10s %-25s %10d %10d %20s %10d\n",
              toupper(result$format),
              result$category,
              result$n_matches,
              result$n_teams,
              substr(top_team_name, 1, 20),
              result$top_elo))
}

cat("\n")
formats_processed <- paste(toupper(formats_to_process), collapse = ", ")
cli::cli_alert_success("All categories processed for: {formats_processed}!")
cli::cli_alert_info("History files saved to: {output_dir}/team_elo_history_*_*.rds")
cat("\n")
