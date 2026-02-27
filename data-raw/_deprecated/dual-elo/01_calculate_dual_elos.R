# 02 Calculate Dual ELOs ----
#
# This script calculates dual-dimension player ELO ratings for cricket:
#   - Run ELO: Measures scoring ability (wicket = 0, worst outcome)
#   - Wicket ELO: Measures survival/strike ability (weighted updates)
#
# Supports:
#   - Formats: T20, ODI, Test (configurable via FORMAT_FILTER)
#   - Genders: mens, womens (separate ELO pools, combined club+international)
#
# NEW: Dynamic K-Factor System (prevents weak-vs-weak ELO inflation)
#   - Dynamic K based on experience (new players learn faster)
#   - Opponent strength adjustment (beating strong players = more gain)
#   - Tier-based starting ELOs (IPL players start higher than minor leagues)
#   - Correlation-based propagation (anchors isolated player pools)
#
# Modes:
#   - FORCE_FULL = TRUE: Always recalculate everything
#   - FORCE_FULL = FALSE: Auto-detect if params changed
#     - Params same: Incremental (process only new deliveries)
#     - Params different: Full recalculation
#
# Output:
#   - {gender}_{format}_player_elo tables populated in DuckDB
#     e.g., mens_t20_player_elo, womens_odi_player_elo
#
# Prerequisites:
#   - Run 01_calibrate_expected_values.R first to set up calibration data

# 1. Setup ----
library(DBI)
library(data.table)
devtools::load_all()

# 2. Configuration ----
FORMAT_GROUPS <- list(
  t20 = c("T20", "IT20"),
  odi = c("ODI", "ODM"),
  test = c("Test", "MDM")
)

# Gender categories (separate ELO pools, combined club+international)
GENDER_CATEGORIES <- list(
  mens = "male",
  womens = "female"
)

FORMAT_FILTER <- "t20" #NULL    # NULL = all formats, or "t20", "odi", "test" for single format
GENDER_FILTER <- "mens" #NULL    # NULL = all genders, or "mens", "womens" for single gender
BATCH_SIZE <- 10000      # Deliveries per batch insert
MATCH_LIMIT <- NULL      # Set to integer to limit matches (for testing)
FORCE_FULL <- TRUE       # If TRUE, always recalculate everything
USE_DYNAMIC_K <- TRUE    # If TRUE, use dynamic K-factors (recommended)
USE_PROPAGATION <- FALSE  # Disabled - old propagation was boosting instead of anchoring
USE_TEAM_ANCHORED_PROPAGATION <- TRUE  # Use team ELO to anchor player ELO
USE_TEAM_ELO_K_MODIFIER <- TRUE        # Blend team ELO into K-factor calculation
USE_CROSS_TIER_K_BOOST <- TRUE         # Boost K when teams from different tiers meet

# Determine formats to process
if (is.null(FORMAT_FILTER)) {
  formats_to_process <- names(FORMAT_GROUPS)
} else {
  formats_to_process <- FORMAT_FILTER
}

# Determine genders to process
if (is.null(GENDER_FILTER)) {
  genders_to_process <- names(GENDER_CATEGORIES)
} else {
  genders_to_process <- GENDER_FILTER
}

cat("\n")
cli::cli_h1("Player ELO Calculation")
cli::cli_alert_info("Genders to process: {paste(toupper(genders_to_process), collapse = ', ')}")
cli::cli_alert_info("Formats to process: {paste(toupper(formats_to_process), collapse = ', ')}")
if (USE_DYNAMIC_K) {
  cli::cli_alert_info("Using DYNAMIC K-factors (experience + opponent strength)")
} else {
  cli::cli_alert_info("Using STATIC K-factors (original system)")
}
if (USE_PROPAGATION) {
  cli::cli_alert_info("Using CORRELATION-BASED propagation (old method, disabled)")
}
if (USE_TEAM_ANCHORED_PROPAGATION) {
  cli::cli_alert_info("Using TEAM-ANCHORED propagation (uses team ELO to anchor players)")
}
if (USE_TEAM_ELO_K_MODIFIER) {
  cli::cli_alert_info("Using TEAM ELO for K-factor modifier (70% team, 30% player)")
}
if (USE_CROSS_TIER_K_BOOST) {
  cli::cli_alert_info("Using CROSS-TIER K-boost (1.3x when tiers differ)")
}
cat("\n")

# 3. Database Connection ----
cli::cli_h2("Connecting to database")
conn <- get_db_connection(read_only = FALSE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
cli::cli_alert_success("Connected to database")

# 4. Process Each Gender and Format ----
for (current_gender in genders_to_process) {

gender_db_value <- GENDER_CATEGORIES[[current_gender]]

cat("\n")
cli::cli_rule("{toupper(current_gender)} CRICKET")
cat("\n")

for (current_format in formats_to_process) {

# Build combined category name for table naming
current_category <- paste0(current_gender, "_", current_format)

cat("\n")
cli::cli_h1("{toupper(current_gender)} {toupper(current_format)} Player ELO Calculation")
cat("\n")

# Build format-specific values
format_match_types <- FORMAT_GROUPS[[current_format]]
match_type_filter <- paste(sprintf("'%s'", tolower(format_match_types)), collapse = ", ")

## 4.1 Build Current Parameters ----
cli::cli_h2("Checking parameters")
current_params <- build_elo_params(current_format)
# Override format to use category (gender_format) for storage/lookup
current_params$format <- current_category
stored_params <- get_stored_elo_params(current_category, conn)

# 4.2 Determine Mode ----
if (FORCE_FULL) {
  mode <- "full"
  cli::cli_alert_info("FORCE_FULL = TRUE, running full recalculation")
} else if (is.null(stored_params)) {
  mode <- "full"
  cli::cli_alert_info("No stored parameters found, running full calculation")
} else if (!elo_params_match(current_params, stored_params)) {
  mode <- "full"
  cli::cli_alert_warning("Parameters changed, running full recalculation")

  # Show what changed
  param_names <- c("k_run", "k_wicket", "k_wicket_survival",
                   "run_score_wicket", "run_score_dot", "run_score_single",
                   "run_score_two", "run_score_three", "run_score_four",
                   "run_score_six", "elo_start", "elo_divisor")
  for (p in param_names) {
    if (abs(current_params[[p]] - stored_params[[p]]) > 1e-6) {
      cli::cli_alert_info("  {p}: {stored_params[[p]]} -> {current_params[[p]]}")
    }
  }
} else {
  mode <- "incremental"
  cli::cli_alert_success("Parameters match, running incremental update")
  cli::cli_alert_info("Last processed: {stored_params$last_match_date} ({stored_params$total_deliveries} deliveries)")
}

# 4.3 Prepare Table ----
if (mode == "full") {
  # Use category (gender_format) for table naming
  create_format_elo_table(current_category, conn, overwrite = TRUE)
}

# 4.4 Load Calibration Data ----
cli::cli_h2("Loading calibration data")
calibration <- get_calibration_data(current_format, conn)

if (is.null(calibration)) {
  cli::cli_alert_danger("No calibration data found!")
  cli::cli_alert_info("Run 01_calibrate_expected_values.R first")
  stop("Calibration data required")
}

cli::cli_alert_success("Calibration loaded: wicket_rate={round(calibration$wicket_rate * 100, 2)}%, mean_runs={round(calibration$mean_runs, 3)}")

# 4.5 Load Deliveries ----
cli::cli_h2("Loading {toupper(current_gender)} {toupper(current_format)} deliveries")

# Build query for this format's matches (include event_name for tier-based starting ELO)
# Filter by gender from matches table
# Include batting_team and bowling_team for team-level correlation
base_query <- sprintf("
  SELECT
    d.delivery_id,
    d.match_id,
    d.match_date,
    d.batter_id,
    d.bowler_id,
    d.batting_team,
    d.bowling_team,
    d.runs_batter,
    d.is_wicket,
    d.is_boundary,
    m.event_name
  FROM cricsheet.deliveries d
  LEFT JOIN cricsheet.matches m ON d.match_id = m.match_id
  WHERE LOWER(d.match_type) IN (%s)
    AND m.gender = '%s'
    AND d.batter_id IS NOT NULL
    AND d.bowler_id IS NOT NULL
", match_type_filter, gender_db_value)

# Add incremental filter if applicable
if (mode == "incremental" && !is.null(stored_params$last_match_date)) {
  # Get deliveries after last processed date
  base_query <- paste0(base_query, sprintf("
    AND d.match_date >= '%s'
  ", stored_params$last_match_date))
}

if (!is.null(MATCH_LIMIT)) {
  # Get limited set of match_ids (filtered by gender)
  match_ids <- DBI::dbGetQuery(conn, sprintf("
    SELECT DISTINCT d.match_id
    FROM cricsheet.deliveries d
    JOIN cricsheet.matches m ON d.match_id = m.match_id
    WHERE LOWER(d.match_type) IN (%s)
      AND m.gender = '%s'
    ORDER BY d.match_date
    LIMIT %d
  ", match_type_filter, gender_db_value, MATCH_LIMIT))$match_id

  placeholders <- paste(rep("?", length(match_ids)), collapse = ", ")
  query <- paste0(base_query, sprintf(" AND d.match_id IN (%s)", placeholders))
  query <- paste0(query, " ORDER BY d.match_date, d.delivery_id")
  deliveries <- DBI::dbGetQuery(conn, query, params = as.list(match_ids))
} else {
  query <- paste0(base_query, " ORDER BY d.match_date, d.delivery_id")
  deliveries <- DBI::dbGetQuery(conn, query)
}

# Convert to data.table for speed
setDT(deliveries)

# For incremental mode, filter out already processed deliveries
if (mode == "incremental" && !is.null(stored_params$last_delivery_id)) {
  existing_ids <- DBI::dbGetQuery(conn, sprintf("
    SELECT delivery_id FROM %s_player_elo WHERE match_date >= '%s'
  ", current_category, stored_params$last_match_date))$delivery_id

  if (length(existing_ids) > 0) {
    deliveries <- deliveries[!delivery_id %in% existing_ids]
    cli::cli_alert_info("Filtered out {length(existing_ids)} already processed deliveries")
  }
}

n_deliveries <- nrow(deliveries)
n_matches <- uniqueN(deliveries$match_id)

cli::cli_alert_success("Loaded {format(n_deliveries, big.mark = ',')} deliveries from {n_matches} matches")

if (n_deliveries == 0) {
  cli::cli_alert_success("No new {toupper(current_gender)} {toupper(current_format)} deliveries to process")
  cli::cli_alert_info("ELO data is up to date for {current_category}")
  next
}

# 4.6 Load Team ELOs ----
if (USE_TEAM_ELO_K_MODIFIER || USE_CROSS_TIER_K_BOOST || USE_TEAM_ANCHORED_PROPAGATION) {
  cli::cli_h2("Loading team ELOs")

  # Get unique match_ids
  unique_match_ids <- unique(deliveries$match_id)

  # Load team ELOs for all matches in our delivery set
  # team_elo stores elo_result (ELO after the match)
  # We use elo_result as an approximation of team strength
  team_elo_query <- sprintf("
    SELECT match_id, team_id, elo_result, event_name
    FROM team_elo
    WHERE match_id IN (%s)
  ", paste(sprintf("'%s'", unique_match_ids), collapse = ", "))

  team_elos_raw <- DBI::dbGetQuery(conn, team_elo_query)
  setDT(team_elos_raw)

  if (nrow(team_elos_raw) > 0) {
    # Build lookup: match_id + team_name (extracted from team_id) -> elo_result
    # team_id format: {team_snake_case}_{gender}_{format}_{type}
    # Extract team_snake_case (everything before _male or _female)
    team_elos_raw[, team_snake := sub("_(male|female)_.*$", "", team_id)]

    # Get event tier from event_name
    team_elos_raw[, event_tier := sapply(event_name, get_event_tier)]

    # Create lookup environment for fast access
    team_elo_lookup <- new.env(hash = TRUE)
    team_tier_lookup <- new.env(hash = TRUE)

    for (i in seq_len(nrow(team_elos_raw))) {
      match_id <- team_elos_raw$match_id[i]
      team_snake <- team_elos_raw$team_snake[i]
      elo <- team_elos_raw$elo_result[i]  # Use elo_result as team strength
      tier <- team_elos_raw$event_tier[i]

      # Store as match_id:team_snake -> elo
      key <- paste0(match_id, ":", team_snake)
      team_elo_lookup[[key]] <- elo
      team_tier_lookup[[key]] <- tier
    }

    cli::cli_alert_success("Loaded team ELOs for {length(unique(team_elos_raw$match_id))} matches")
  } else {
    cli::cli_alert_warning("No team ELOs found - will skip team-based adjustments")
    team_elo_lookup <- new.env(hash = TRUE)
    team_tier_lookup <- new.env(hash = TRUE)
  }

  # Function to convert team name to snake_case for lookup
  team_to_snake <- function(team_name) {
    tolower(gsub("'", "", gsub(" ", "_", team_name)))
  }
}

# 4.7 Initialize Player ELO State ----
cli::cli_h2("Initializing player ELO state")

# Get all unique players from deliveries to process
all_batters <- unique(deliveries$batter_id)
all_bowlers <- unique(deliveries$bowler_id)
all_players <- unique(c(all_batters, all_bowlers))

cli::cli_alert_info("{length(all_batters)} unique batters, {length(all_bowlers)} unique bowlers in new data")

# For dynamic K: track deliveries per player (as batter and bowler separately)
player_batter_deliveries <- new.env(hash = TRUE)
player_bowler_deliveries <- new.env(hash = TRUE)
for (player in all_players) {
  player_batter_deliveries[[player]] <- 0L
  player_bowler_deliveries[[player]] <- 0L
}

# For tier-based starting ELO: get first event for each player
# This is only used when USE_DYNAMIC_K is TRUE
if (USE_DYNAMIC_K) {
  cli::cli_alert_info("Building first-event lookup for tier-based starting ELOs...")

  # Get first event for batters (sorted by match_date, take first)
  setkey(deliveries, match_date, delivery_id)
  batter_first <- deliveries[, list(first_event = event_name[1]), by = batter_id]
  setnames(batter_first, "batter_id", "player_id")

  # Get first event for bowlers
  bowler_first <- deliveries[, list(first_event = event_name[1]), by = bowler_id]
  setnames(bowler_first, "bowler_id", "player_id")

  # Combine (use batter's first if they appear as both)
  first_events <- rbind(batter_first, bowler_first)
  first_events <- first_events[!duplicated(player_id)]

  # Create lookup environment
  player_first_event <- new.env(hash = TRUE)
  for (i in seq_len(nrow(first_events))) {
    player_first_event[[first_events$player_id[i]]] <- first_events$first_event[i]
  }
  cli::cli_alert_success("Built first-event lookup for {nrow(first_events)} players")
}

# Initialize or load ELO states
# CRITICAL: Separate batting and bowling ELOs - each player has 4 ELO values
# - Batter run ELO: measures batting scoring ability
# - Batter wicket ELO: measures batting survival ability
# - Bowler run ELO: measures bowling run prevention
# - Bowler wicket ELO: measures bowling wicket-taking ability
if (mode == "incremental") {
  # Load existing player state
  cli::cli_alert_info("Loading existing player ELO state...")
  player_state <- get_all_player_elo_state(current_category, conn)

  if (!is.null(player_state)) {
    # Note: get_all_player_elo_state returns separate batter/bowler ELOs
    player_batter_run_elo <- player_state$batter_run_elo
    player_batter_wicket_elo <- player_state$batter_wicket_elo
    player_bowler_run_elo <- player_state$bowler_run_elo
    player_bowler_wicket_elo <- player_state$bowler_wicket_elo

    # Initialize any new batters not in state
    for (player in all_batters) {
      if (!exists(player, envir = player_batter_run_elo)) {
        start_elo <- if (USE_DYNAMIC_K && exists(player, envir = player_first_event)) {
          get_player_tier_starting_elo(player_first_event[[player]])
        } else {
          DUAL_ELO_START
        }
        player_batter_run_elo[[player]] <- start_elo
        player_batter_wicket_elo[[player]] <- start_elo
      }
    }

    # Initialize any new bowlers not in state
    for (player in all_bowlers) {
      if (!exists(player, envir = player_bowler_run_elo)) {
        start_elo <- if (USE_DYNAMIC_K && exists(player, envir = player_first_event)) {
          get_player_tier_starting_elo(player_first_event[[player]])
        } else {
          DUAL_ELO_START
        }
        player_bowler_run_elo[[player]] <- start_elo
        player_bowler_wicket_elo[[player]] <- start_elo
      }
    }
    cli::cli_alert_success("Loaded state for {player_state$n_batters} batters, {player_state$n_bowlers} bowlers")
  } else {
    # Fallback to fresh state
    player_batter_run_elo <- new.env(hash = TRUE)
    player_batter_wicket_elo <- new.env(hash = TRUE)
    player_bowler_run_elo <- new.env(hash = TRUE)
    player_bowler_wicket_elo <- new.env(hash = TRUE)

    for (player in all_batters) {
      start_elo <- if (USE_DYNAMIC_K && exists(player, envir = player_first_event)) {
        get_player_tier_starting_elo(player_first_event[[player]])
      } else {
        DUAL_ELO_START
      }
      player_batter_run_elo[[player]] <- start_elo
      player_batter_wicket_elo[[player]] <- start_elo
    }
    for (player in all_bowlers) {
      start_elo <- if (USE_DYNAMIC_K && exists(player, envir = player_first_event)) {
        get_player_tier_starting_elo(player_first_event[[player]])
      } else {
        DUAL_ELO_START
      }
      player_bowler_run_elo[[player]] <- start_elo
      player_bowler_wicket_elo[[player]] <- start_elo
    }
    cli::cli_alert_warning("Could not load state, initialized {length(all_batters)} batters and {length(all_bowlers)} bowlers fresh")
  }
} else {
  # Fresh state for full recalculation - SEPARATE batting and bowling ELOs
  player_batter_run_elo <- new.env(hash = TRUE)
  player_batter_wicket_elo <- new.env(hash = TRUE)
  player_bowler_run_elo <- new.env(hash = TRUE)
  player_bowler_wicket_elo <- new.env(hash = TRUE)

  if (USE_DYNAMIC_K) {
    # Use tier-based starting ELOs
    tier_counts <- c(T1 = 0L, T2 = 0L, T3 = 0L, T4 = 0L)

    # Initialize batters
    for (player in all_batters) {
      first_event <- if (exists(player, envir = player_first_event)) {
        player_first_event[[player]]
      } else {
        NA
      }
      start_elo <- get_player_tier_starting_elo(first_event)
      player_batter_run_elo[[player]] <- start_elo
      player_batter_wicket_elo[[player]] <- start_elo

      tier <- get_event_tier(first_event)
      tier_counts[tier] <- tier_counts[tier] + 1L
    }

    # Initialize bowlers
    for (player in all_bowlers) {
      first_event <- if (exists(player, envir = player_first_event)) {
        player_first_event[[player]]
      } else {
        NA
      }
      start_elo <- get_player_tier_starting_elo(first_event)
      player_bowler_run_elo[[player]] <- start_elo
      player_bowler_wicket_elo[[player]] <- start_elo
    }

    cli::cli_alert_success("Initialized {length(all_batters)} batters and {length(all_bowlers)} bowlers with tier-based ELOs")
    cli::cli_alert_info("Tier distribution (batters): T1={tier_counts[1]}, T2={tier_counts[2]}, T3={tier_counts[3]}, T4={tier_counts[4]}")
  } else {
    # Use static starting ELO
    for (player in all_batters) {
      player_batter_run_elo[[player]] <- DUAL_ELO_START
      player_batter_wicket_elo[[player]] <- DUAL_ELO_START
    }
    for (player in all_bowlers) {
      player_bowler_run_elo[[player]] <- DUAL_ELO_START
      player_bowler_wicket_elo[[player]] <- DUAL_ELO_START
    }
    cli::cli_alert_success("Initialized {length(all_batters)} batters and {length(all_bowlers)} bowlers at ELO {DUAL_ELO_START}")
  }
}

# 4.8 Pre-extract columns for faster access ----
delivery_ids <- deliveries$delivery_id
match_ids <- deliveries$match_id
match_dates <- deliveries$match_date
batter_ids <- deliveries$batter_id
bowler_ids <- deliveries$bowler_id
batting_teams <- deliveries$batting_team
bowling_teams <- deliveries$bowling_team
runs_vec <- deliveries$runs_batter
wicket_vec <- deliveries$is_wicket
boundary_vec <- deliveries$is_boundary
event_names <- deliveries$event_name

# 4.9 Initialize Correlation Tracking ----
# NOTE: We use TEAM-LEVEL correlation (not player-level) for performance
# Player-level with 8000+ players would be ~65M comparisons
# Team-level with ~500 teams is ~250K comparisons
if (USE_PROPAGATION) {
  cli::cli_h2("Initializing team-level correlation tracking")

  # Get all unique teams
  all_teams <- unique(c(batting_teams, bowling_teams))
  all_teams <- all_teams[!is.na(all_teams)]
  cli::cli_alert_info("Found {length(all_teams)} unique teams")

  # Track opponents for each team (teams faced)
  team_opponents <- list()
  team_events <- list()
  team_matchups <- list()

  # Pre-allocate empty lists for all teams
  for (team in all_teams) {
    team_opponents[[team]] <- character(0)
    team_events[[team]] <- character(0)
    team_matchups[[team]] <- list()
  }

  # Track which players belong to which teams (for propagation back to players)
  player_teams <- list()
  for (player in all_players) {
    player_teams[[player]] <- character(0)
  }

  # Store ELO updates per player for aggregation
  prop_batter_ids <- character(n_deliveries)
  prop_bowler_ids <- character(n_deliveries)
  prop_batting_teams <- character(n_deliveries)
  prop_bowling_teams <- character(n_deliveries)
  prop_run_update_batter <- numeric(n_deliveries)
  prop_run_update_bowler <- numeric(n_deliveries)
  prop_wicket_update_batter <- numeric(n_deliveries)
  prop_wicket_update_bowler <- numeric(n_deliveries)

  cli::cli_alert_success("Team-level tracking enabled for {length(all_teams)} teams")
}

# 4.10 Pre-allocate result matrix ----
cli::cli_h2("Processing deliveries")

# Pre-allocate result storage (much faster than growing lists)
result_mat <- matrix(NA_real_, nrow = n_deliveries, ncol = 12)
colnames(result_mat) <- c(
  "batter_run_before", "batter_run_after",
  "bowler_run_before", "bowler_run_after",
  "batter_wicket_before", "batter_wicket_after",
  "bowler_wicket_before", "bowler_wicket_after",
  "exp_runs", "exp_wicket", "actual_runs", "is_wicket"
)

# Get K factors from current params (for static mode fallback)
k_run_static <- current_params$k_run
k_wicket_static <- current_params$k_wicket
k_wicket_survival_static <- current_params$k_wicket_survival

# Progress tracking
cli::cli_progress_bar("Calculating ELOs", total = n_deliveries)

for (i in seq_len(n_deliveries)) {
  batter_id <- batter_ids[i]
  bowler_id <- bowler_ids[i]
  runs <- runs_vec[i]
  is_wicket <- wicket_vec[i]
  is_boundary <- if (!is.na(boundary_vec[i])) boundary_vec[i] else FALSE

  # Get current ELOs - SEPARATE batting and bowling ELOs
  # Batter's ELO comes from their BATTER state (how they perform when batting)
  # Bowler's ELO comes from their BOWLER state (how they perform when bowling)
  batter_run_before <- player_batter_run_elo[[batter_id]]
  bowler_run_before <- player_bowler_run_elo[[bowler_id]]
  batter_wicket_before <- player_batter_wicket_elo[[batter_id]]
  bowler_wicket_before <- player_bowler_wicket_elo[[bowler_id]]

  # Calculate expected values (calibrated)
  exp_runs <- calculate_expected_runs_calibrated(batter_run_before, bowler_run_before, calibration)
  exp_wicket <- calculate_expected_wicket_calibrated(batter_wicket_before, bowler_wicket_before, calibration)

  # Calculate actual outcomes
  actual_run_score <- calculate_run_outcome_score(runs, is_wicket, is_boundary)
  actual_wicket <- if (is_wicket) 1 else 0

  # === GET TEAM ELOs (for K-modifier and cross-tier boost) ===
  batting_team_elo <- NULL
  bowling_team_elo <- NULL
  batting_team_tier <- NULL
  bowling_team_tier <- NULL

  if (USE_TEAM_ELO_K_MODIFIER || USE_CROSS_TIER_K_BOOST) {
    batting_team <- batting_teams[i]
    bowling_team <- bowling_teams[i]
    match_id <- match_ids[i]

    # Lookup team ELOs
    bat_key <- paste0(match_id, ":", team_to_snake(batting_team))
    bowl_key <- paste0(match_id, ":", team_to_snake(bowling_team))

    if (exists(bat_key, envir = team_elo_lookup)) {
      batting_team_elo <- team_elo_lookup[[bat_key]]
      batting_team_tier <- team_tier_lookup[[bat_key]]
    }
    if (exists(bowl_key, envir = team_elo_lookup)) {
      bowling_team_elo <- team_elo_lookup[[bowl_key]]
      bowling_team_tier <- team_tier_lookup[[bowl_key]]
    }
  }

  # === CALCULATE K-FACTORS ===
  # CRITICAL: For zero-sum ELO, both players MUST use the same K-factor per delivery.
  # We compute individual K's based on experience/opponent, then average them.
  if (USE_DYNAMIC_K) {
    # Get player delivery counts
    batter_balls <- player_batter_deliveries[[batter_id]]
    bowler_balls <- player_bowler_deliveries[[bowler_id]]

    # Dynamic K based on experience (base K for each player)
    k_run_batter_base <- get_player_dynamic_k_run(batter_balls, current_format)
    k_run_bowler_base <- get_player_dynamic_k_run(bowler_balls, current_format)
    k_wicket_batter_base <- get_player_dynamic_k_wicket(batter_balls, current_format)
    k_wicket_bowler_base <- get_player_dynamic_k_wicket(bowler_balls, current_format)

    # ZERO-SUM FIX: Use SHARED K-factor for both batter and bowler
    # Different K's break zero-sum: total_change = (K_batter - K_bowler) * delta ≠ 0
    # With shared K: batter_update + bowler_update = K*delta + K*(-delta) = 0 ✓

    # Step 1: Average the dynamic K's to get base shared K
    k_run_base <- (k_run_batter_base + k_run_bowler_base) / 2
    k_wicket_base <- (k_wicket_batter_base + k_wicket_bowler_base) / 2

    # Step 2: Apply opponent modifier to shared K based on who "won"
    # NEW: Pass team ELO for more accurate opponent strength assessment
    # For run ELO: batter "wins" if scores above expectation
    batter_run_won <- actual_run_score > exp_runs
    if (batter_run_won) {
      # Batter won - use bowler's team ELO to assess opponent strength
      k_run_mod <- get_player_opponent_k_modifier(
        batter_run_before, bowler_run_before, TRUE,
        opponent_team_elo = if (USE_TEAM_ELO_K_MODIFIER) bowling_team_elo else NULL
      )
    } else {
      # Bowler won - use batter's team ELO to assess opponent strength
      k_run_mod <- get_player_opponent_k_modifier(
        bowler_run_before, batter_run_before, TRUE,
        opponent_team_elo = if (USE_TEAM_ELO_K_MODIFIER) batting_team_elo else NULL
      )
    }
    k_run <- k_run_base * k_run_mod

    # Step 3: Apply cross-tier K-boost (when teams from different tiers meet)
    if (USE_CROSS_TIER_K_BOOST && !is.null(batting_team_tier) && !is.null(bowling_team_tier)) {
      tier_boost <- get_player_cross_tier_k_multiplier(batting_team_tier, bowling_team_tier)
      k_run <- k_run * tier_boost
      k_wicket_base <- k_wicket_base * tier_boost  # Also boost wicket K
    }

    # For wicket ELO: batter "wins" if survives, bowler "wins" if gets wicket
    if (is_wicket) {
      # Bowler won - use batter's team ELO
      k_wicket_mod <- get_player_opponent_k_modifier(
        bowler_wicket_before, batter_wicket_before, TRUE,
        opponent_team_elo = if (USE_TEAM_ELO_K_MODIFIER) batting_team_elo else NULL
      )
    } else {
      # Batter survived - use bowler's team ELO
      k_wicket_mod <- get_player_opponent_k_modifier(
        batter_wicket_before, bowler_wicket_before, TRUE,
        opponent_team_elo = if (USE_TEAM_ELO_K_MODIFIER) bowling_team_elo else NULL
      )
    }
    k_wicket <- k_wicket_base * k_wicket_mod

    # Update delivery counts
    player_batter_deliveries[[batter_id]] <- batter_balls + 1L
    player_bowler_deliveries[[bowler_id]] <- bowler_balls + 1L
  } else {
    # Static K-factors (original system) - shared K for zero-sum
    k_run <- k_run_static
    k_wicket <- if (is_wicket) k_wicket_static else k_wicket_survival_static
  }

  # === RUN ELO UPDATE (ZERO-SUM: same K for both) ===
  run_update_batter <- k_run * (actual_run_score - exp_runs)
  run_update_bowler <- k_run * ((1 - actual_run_score) - (1 - exp_runs))
  # Note: (1-a) - (1-e) = e - a = -(a - e), so bowler_update = -batter_update ✓

  batter_run_after <- batter_run_before + run_update_batter
  bowler_run_after <- bowler_run_before + run_update_bowler

  # === WICKET ELO UPDATE (ZERO-SUM: same K for both) ===
  batter_wicket_actual <- 1 - actual_wicket
  bowler_wicket_actual <- actual_wicket
  batter_wicket_exp <- 1 - exp_wicket
  bowler_wicket_exp <- exp_wicket

  wicket_update_batter <- k_wicket * (batter_wicket_actual - batter_wicket_exp)
  wicket_update_bowler <- k_wicket * (bowler_wicket_actual - bowler_wicket_exp)

  batter_wicket_after <- batter_wicket_before + wicket_update_batter
  bowler_wicket_after <- bowler_wicket_before + wicket_update_bowler

  # Update state - SEPARATE batting and bowling ELOs
  # Batter's ELO updates go to their BATTER state
  player_batter_run_elo[[batter_id]] <- batter_run_after
  player_batter_wicket_elo[[batter_id]] <- batter_wicket_after
  # Bowler's ELO updates go to their BOWLER state
  player_bowler_run_elo[[bowler_id]] <- bowler_run_after
  player_bowler_wicket_elo[[bowler_id]] <- bowler_wicket_after

  # Track history for team-level correlation-based propagation
  if (USE_PROPAGATION) {
    event_name <- event_names[i]
    batting_team <- batting_teams[i]
    bowling_team <- bowling_teams[i]

    # Track team-level opponents and events (only once per match between teams)
    # Use a simple check - we'll deduplicate later when building matrix
    if (!is.na(batting_team) && !is.na(bowling_team)) {
      team_opponents[[batting_team]] <- c(team_opponents[[batting_team]], bowling_team)
      team_opponents[[bowling_team]] <- c(team_opponents[[bowling_team]], batting_team)
      team_events[[batting_team]] <- c(team_events[[batting_team]], event_name)
      team_events[[bowling_team]] <- c(team_events[[bowling_team]], event_name)

      # Track matchup counts
      if (is.null(team_matchups[[batting_team]][[bowling_team]])) {
        team_matchups[[batting_team]][[bowling_team]] <- 1L
      } else {
        team_matchups[[batting_team]][[bowling_team]] <- team_matchups[[batting_team]][[bowling_team]] + 1L
      }
      if (is.null(team_matchups[[bowling_team]][[batting_team]])) {
        team_matchups[[bowling_team]][[batting_team]] <- 1L
      } else {
        team_matchups[[bowling_team]][[batting_team]] <- team_matchups[[bowling_team]][[batting_team]] + 1L
      }

      # Track player-team affiliations
      player_teams[[batter_id]] <- c(player_teams[[batter_id]], batting_team)
      player_teams[[bowler_id]] <- c(player_teams[[bowler_id]], bowling_team)
    }

    # Store updates for propagation
    prop_batter_ids[i] <- batter_id
    prop_bowler_ids[i] <- bowler_id
    prop_batting_teams[i] <- batting_team
    prop_bowling_teams[i] <- bowling_team
    prop_run_update_batter[i] <- run_update_batter
    prop_run_update_bowler[i] <- run_update_bowler
    prop_wicket_update_batter[i] <- wicket_update_batter
    prop_wicket_update_bowler[i] <- wicket_update_bowler
  }

  # Store in result matrix
  result_mat[i, ] <- c(
    batter_run_before, batter_run_after,
    bowler_run_before, bowler_run_after,
    batter_wicket_before, batter_wicket_after,
    bowler_wicket_before, bowler_wicket_after,
    exp_runs, exp_wicket, runs, is_wicket
  )

  if (i %% 10000 == 0) {
    cli::cli_progress_update(set = i)
  }
}

cli::cli_progress_done()

# 4.11 Apply Team-Level Correlation-Based Propagation ----
# NOTE: This old propagation code uses the legacy shared ELO approach and is DISABLED.
# If re-enabling, refactor to use separated batter/bowler ELOs (player_batter_run_elo, etc.)
if (USE_PROPAGATION) {
  cli::cli_alert_danger("Old propagation code is incompatible with separated batting/bowling ELOs!")
  cli::cli_alert_warning("Skipping old propagation - use USE_TEAM_ANCHORED_PROPAGATION instead")
  USE_PROPAGATION <- FALSE  # Force disable
}
if (FALSE) {  # OLD CODE - needs refactoring for separated ELOs
  cat("\n")
  cli::cli_h2("Applying Team-Level Correlation-Based Propagation")

  # Step 1: Calculate average ELO per team (using data.table)
  cli::cli_alert_info("Calculating average ELO per team (data.table)...")

  # Build player-team affiliations from tracking data
  player_team_list <- lapply(names(player_teams), function(p) {
    teams <- player_teams[[p]]
    if (length(teams) > 0) {
      data.table(player_id = p, team = teams)
    } else {
      NULL
    }
  })
  player_team_dt <- rbindlist(player_team_list[!sapply(player_team_list, is.null)])

  # Find primary team (most frequent) for each player
  primary_teams <- player_team_dt[, .N, by = .(player_id, team)][
    , .SD[which.max(N)], by = player_id
  ][, .(player_id, primary_team = team)]

  # Build player ELO data.table
  player_elo_dt <- data.table(
    player_id = names(player_run_elo),
    run_elo = unlist(mget(names(player_run_elo), envir = player_run_elo)),
    wicket_elo = unlist(mget(names(player_wicket_elo), envir = player_wicket_elo))
  )

  # Merge with primary teams
  player_elo_dt <- merge(player_elo_dt, primary_teams, by = "player_id", all.x = TRUE)

  # Calculate average team ELOs
  team_avg_dt <- player_elo_dt[
    !is.na(primary_team),
    .(avg_run_elo = mean(run_elo, na.rm = TRUE),
      avg_wicket_elo = mean(wicket_elo, na.rm = TRUE),
      player_count = .N),
    by = primary_team
  ]

  # Convert to lists for compatibility
  team_avg_run_elo <- setNames(as.list(team_avg_dt$avg_run_elo), team_avg_dt$primary_team)
  team_avg_wicket_elo <- setNames(as.list(team_avg_dt$avg_wicket_elo), team_avg_dt$primary_team)
  team_player_count <- setNames(as.list(team_avg_dt$player_count), team_avg_dt$primary_team)

  # Keep primary_teams for later use
  player_primary_team <- setNames(as.list(primary_teams$primary_team), primary_teams$player_id)

  cli::cli_alert_success("Calculated average ELOs for {nrow(team_avg_dt)} teams")

  # Step 2: Build team-level correlation matrix (reuse team_correlation.R functions)
  cli::cli_alert_info("Building team correlation matrix...")

  team_corr_matrix <- build_correlation_matrix(
    all_team_ids = all_teams,
    team_opponents = team_opponents,
    team_events = team_events,
    team_matchups = team_matchups,
    threshold = PLAYER_CORR_THRESHOLD,
    w_opponents = PLAYER_CORR_W_OPPONENTS,
    w_events = PLAYER_CORR_W_EVENTS,
    w_direct = PLAYER_CORR_W_DIRECT
  )

  # Step 3: Calculate team-level ELO adjustments using TEAM ELO APPROACH
  # Instead of summing player delivery deltas (which compound to huge values),
  # use bounded match-level win/loss deltas like team ELO does.
  cli::cli_alert_info("Calculating team deltas using team ELO approach (bounded win/loss)...")

  # Query match outcomes from database
  match_outcomes <- DBI::dbGetQuery(conn, sprintf("
    SELECT match_id, team1, team2, outcome_winner
    FROM cricsheet.matches
    WHERE match_id IN (%s)
      AND outcome_winner IS NOT NULL
      AND outcome_winner != ''
  ", paste0("'", unique(match_ids), "'", collapse = ",")))

  cli::cli_alert_info("Found {nrow(match_outcomes)} matches with outcomes")

  # Calculate bounded deltas per match (like team ELO)
  # Use a fixed delta of ~15 points per match (middle of typical 5-32 range)
  # Winners get +delta, losers get -delta
  MATCH_DELTA <- 15

  # Build match-level team deltas
  match_deltas_list <- lapply(seq_len(nrow(match_outcomes)), function(i) {
    m <- match_outcomes[i, ]
    winner <- m$outcome_winner
    team1 <- m$team1
    team2 <- m$team2
    loser <- if (winner == team1) team2 else team1

    # Winner gets +delta, loser gets -delta
    data.table(
      match_id = c(m$match_id, m$match_id),
      team = c(winner, loser),
      run_delta = c(MATCH_DELTA, -MATCH_DELTA),
      wicket_delta = c(MATCH_DELTA, -MATCH_DELTA)
    )
  })

  match_deltas <- rbindlist(match_deltas_list)
  cli::cli_alert_info("Calculated deltas for {nrow(match_deltas)/2} matches")

  # Sum deltas per team across all matches
  team_deltas <- match_deltas[,
    .(run_delta = sum(run_delta),
      wicket_delta = sum(wicket_delta),
      match_count = .N / 2),  # Each match has 2 entries
    by = team
  ]

  # Convert to lists for compatibility with existing code
  team_run_delta <- setNames(as.list(team_deltas$run_delta), team_deltas$team)
  team_wicket_delta <- setNames(as.list(team_deltas$wicket_delta), team_deltas$team)
  team_match_count <- setNames(as.list(team_deltas$match_count), team_deltas$team)

  cli::cli_alert_success("Team deltas calculated from {nrow(team_deltas)} teams")

  # Show top team deltas (should be bounded, not millions)
  top_deltas <- team_deltas[order(-abs(run_delta))][1:min(5, nrow(team_deltas))]
  cli::cli_alert_info("Top team run deltas: {paste(sprintf('%s=%.0f', top_deltas$team, top_deltas$run_delta), collapse = ', ')}")

  # Step 4: Calculate propagation adjustments per team
  team_run_adj <- list()
  team_wicket_adj <- list()

  for (team in all_teams) {
    run_adj <- 0
    wicket_adj <- 0

    if (team %in% names(team_corr_matrix)) {
      correlates <- team_corr_matrix[[team]]

      for (corr_team in names(correlates)) {
        corr <- correlates[[corr_team]]

        # Propagate correlated team's delta to this team
        corr_run_delta <- team_run_delta[[corr_team]] %||% 0
        corr_wicket_delta <- team_wicket_delta[[corr_team]] %||% 0

        run_adj <- run_adj + corr_run_delta * corr * PLAYER_PROPAGATION_FACTOR
        wicket_adj <- wicket_adj + corr_wicket_delta * corr * PLAYER_PROPAGATION_FACTOR
      }
    }

    team_run_adj[[team]] <- run_adj
    team_wicket_adj[[team]] <- wicket_adj
  }

  # Step 5: Apply team adjustments back to players (using data.table)
  cli::cli_alert_info("Applying team adjustments to players (data.table)...")

  # Build team adjustment data.table
  team_adj_dt <- data.table(
    team = names(team_run_adj),
    run_adj = unlist(team_run_adj),
    wicket_adj = unlist(team_wicket_adj)
  )

  # Merge with primary_teams to get player adjustments
  player_adj_dt <- merge(
    primary_teams,
    team_adj_dt,
    by.x = "primary_team", by.y = "team",
    all.x = TRUE
  )
  player_adj_dt[is.na(run_adj), run_adj := 0]
  player_adj_dt[is.na(wicket_adj), wicket_adj := 0]

  # Convert to named vectors for fast lookup
  prop_run_adj_vec <- setNames(player_adj_dt$run_adj, player_adj_dt$player_id)
  prop_wicket_adj_vec <- setNames(player_adj_dt$wicket_adj, player_adj_dt$player_id)

  # Update player state environments
  for (i in seq_len(nrow(player_adj_dt))) {
    p <- player_adj_dt$player_id[i]
    if (p %in% names(player_run_elo)) {
      player_run_elo[[p]] <- player_run_elo[[p]] + player_adj_dt$run_adj[i]
      player_wicket_elo[[p]] <- player_wicket_elo[[p]] + player_adj_dt$wicket_adj[i]
    }
  }

  # Report top adjustments
  if (length(prop_run_adj_vec) > 0) {
    n_nonzero <- sum(abs(prop_run_adj_vec) > 0.01)
    top_adj <- sort(abs(prop_run_adj_vec), decreasing = TRUE)[1:min(5, length(prop_run_adj_vec))]
    cli::cli_alert_info("{n_nonzero} players had propagation adjustments")
    cli::cli_alert_info("Top run ELO adjustments: {paste(round(top_adj, 1), collapse = ', ')} points")
  }

  # Update final _after values in result_mat using data.table (FAST)
  cli::cli_alert_info("Applying propagation adjustments to result matrix (data.table)...")

  # Find last occurrence of each batter and bowler
  idx_dt <- data.table(
    idx = seq_len(n_deliveries),
    batter_id = batter_ids,
    bowler_id = bowler_ids
  )

  # Last occurrence for each batter
  last_batter_idx <- idx_dt[, .(last_idx = max(idx)), by = batter_id]

  # Last occurrence for each bowler
  last_bowler_idx <- idx_dt[, .(last_idx = max(idx)), by = bowler_id]

  # Apply batter adjustments to their last occurrence
  for (i in seq_len(nrow(last_batter_idx))) {
    batter <- last_batter_idx$batter_id[i]
    idx <- last_batter_idx$last_idx[i]
    adj_run <- prop_run_adj_vec[batter]
    adj_wicket <- prop_wicket_adj_vec[batter]
    if (!is.na(adj_run)) {
      result_mat[idx, 2] <- result_mat[idx, 2] + adj_run   # batter_run_after
      result_mat[idx, 6] <- result_mat[idx, 6] + adj_wicket # batter_wicket_after
    }
  }

  # Apply bowler adjustments to their last occurrence
  for (i in seq_len(nrow(last_bowler_idx))) {
    bowler <- last_bowler_idx$bowler_id[i]
    idx <- last_bowler_idx$last_idx[i]
    adj_run <- prop_run_adj_vec[bowler]
    adj_wicket <- prop_wicket_adj_vec[bowler]
    if (!is.na(adj_run)) {
      result_mat[idx, 4] <- result_mat[idx, 4] + adj_run   # bowler_run_after
      result_mat[idx, 8] <- result_mat[idx, 8] + adj_wicket # bowler_wicket_after
    }
  }

  # Keep prop_run_adj as list for output reporting
  prop_run_adj <- as.list(prop_run_adj_vec)
  prop_wicket_adj <- as.list(prop_wicket_adj_vec)

  cli::cli_alert_success("Team-level propagation complete!")
}  # End of if (FALSE) block for old propagation

# 4.12 Apply Team-Anchored Propagation ----
if (USE_TEAM_ANCHORED_PROPAGATION) {
  cat("\n")
  cli::cli_h2("Applying Team-Anchored Propagation (uses team ELO to anchor players)")

  # Step 1: Build primary team affiliations for each player
  cli::cli_alert_info("Building player-team affiliations...")

  if (!exists("player_teams") || length(player_teams) == 0) {
    # Build player-team affiliations from deliveries
    player_teams <- list()
    for (i in seq_len(n_deliveries)) {
      batter <- batter_ids[i]
      bowler <- bowler_ids[i]
      batting_team <- batting_teams[i]
      bowling_team <- bowling_teams[i]

      player_teams[[batter]] <- c(player_teams[[batter]], batting_team)
      player_teams[[bowler]] <- c(player_teams[[bowler]], bowling_team)
    }
  }

  # Find primary team for each player (most frequent)
  player_primary_teams <- character(0)
  for (p in names(player_teams)) {
    teams <- player_teams[[p]]
    if (length(teams) > 0) {
      team_counts <- table(teams)
      player_primary_teams[p] <- names(team_counts)[which.max(team_counts)]
    }
  }

  cli::cli_alert_success("Built primary teams for {length(player_primary_teams)} players")

  # Step 2: Get final team ELOs from team_elo table
  cli::cli_alert_info("Loading final team ELOs from team_elo table...")

  unique_teams <- unique(player_primary_teams)
  unique_teams_snake <- sapply(unique_teams, team_to_snake)

  # Query latest team ELO for each team
  # Build a query that gets the most recent ELO for each team
  team_final_elos_query <- sprintf("
    SELECT
      team_id,
      elo_result,
      match_date
    FROM team_elo
    WHERE team_id LIKE '%%_%s_%%'
    ORDER BY match_date DESC
  ", current_format)

  team_final_elos_raw <- DBI::dbGetQuery(conn, team_final_elos_query)
  setDT(team_final_elos_raw)

  if (nrow(team_final_elos_raw) > 0) {
    # Extract team snake name and get most recent
    team_final_elos_raw[, team_snake := sub("_(male|female)_.*$", "", team_id)]
    team_final_elos <- team_final_elos_raw[, .SD[1], by = team_snake]

    # Build lookup from team name to team ELO
    team_elo_final_lookup <- setNames(
      as.list(team_final_elos$elo_result),
      team_final_elos$team_snake
    )

    cli::cli_alert_success("Loaded final ELOs for {length(team_elo_final_lookup)} teams")

    # Step 3: Build SEPARATE player_elos lists for batters and bowlers
    # Batting ELOs and bowling ELOs are distinct - propagate them separately
    player_batter_elos_list <- list()
    for (p in ls(player_batter_run_elo)) {
      player_batter_elos_list[[p]] <- list(
        run_elo = player_batter_run_elo[[p]],
        wicket_elo = player_batter_wicket_elo[[p]]
      )
    }

    player_bowler_elos_list <- list()
    for (p in ls(player_bowler_run_elo)) {
      player_bowler_elos_list[[p]] <- list(
        run_elo = player_bowler_run_elo[[p]],
        wicket_elo = player_bowler_wicket_elo[[p]]
      )
    }

    # Build team_elos named vector (map team name -> team ELO)
    team_elos_for_prop <- numeric(0)
    for (team_name in unique_teams) {
      team_snake <- team_to_snake(team_name)
      if (team_snake %in% names(team_elo_final_lookup)) {
        team_elos_for_prop[team_name] <- team_elo_final_lookup[[team_snake]]
      }
    }

    cli::cli_alert_info("Mapped {length(team_elos_for_prop)} teams to ELOs")

    # Step 4: Apply team-anchored propagation SEPARATELY for batters and bowlers
    cli::cli_alert_info("Applying propagation to batter ELOs...")
    player_batter_elos_list <- apply_full_team_anchored_propagation(
      player_batter_elos_list,
      team_elos_for_prop,
      player_primary_teams,
      propagation_factor = 0.2
    )

    cli::cli_alert_info("Applying propagation to bowler ELOs...")
    player_bowler_elos_list <- apply_full_team_anchored_propagation(
      player_bowler_elos_list,
      team_elos_for_prop,
      player_primary_teams,
      propagation_factor = 0.2
    )

    # Step 5: Update player state environments from propagation
    for (p in names(player_batter_elos_list)) {
      player_batter_run_elo[[p]] <- player_batter_elos_list[[p]]$run_elo
      player_batter_wicket_elo[[p]] <- player_batter_elos_list[[p]]$wicket_elo
    }
    for (p in names(player_bowler_elos_list)) {
      player_bowler_run_elo[[p]] <- player_bowler_elos_list[[p]]$run_elo
      player_bowler_wicket_elo[[p]] <- player_bowler_elos_list[[p]]$wicket_elo
    }

    # Step 6: Update final _after values in result_mat
    cli::cli_alert_info("Applying team-anchored adjustments to result matrix...")

    # Find last occurrence of each player and update with propagated ELOs
    idx_dt <- data.table(
      idx = seq_len(n_deliveries),
      batter_id = batter_ids,
      bowler_id = bowler_ids
    )

    last_batter_idx <- idx_dt[, .(last_idx = max(idx)), by = batter_id]
    last_bowler_idx <- idx_dt[, .(last_idx = max(idx)), by = bowler_id]

    # Update BATTER ELOs (from batter state)
    for (i in seq_len(nrow(last_batter_idx))) {
      batter <- last_batter_idx$batter_id[i]
      idx <- last_batter_idx$last_idx[i]
      result_mat[idx, 2] <- player_batter_run_elo[[batter]]     # batter_run_after
      result_mat[idx, 6] <- player_batter_wicket_elo[[batter]]  # batter_wicket_after
    }

    # Update BOWLER ELOs (from bowler state)
    for (i in seq_len(nrow(last_bowler_idx))) {
      bowler <- last_bowler_idx$bowler_id[i]
      idx <- last_bowler_idx$last_idx[i]
      result_mat[idx, 4] <- player_bowler_run_elo[[bowler]]     # bowler_run_after
      result_mat[idx, 8] <- player_bowler_wicket_elo[[bowler]]  # bowler_wicket_after
    }

    cli::cli_alert_success("Team-anchored propagation complete!")
  } else {
    cli::cli_alert_warning("No team ELOs found - skipping team-anchored propagation")
  }
}

# 4.13 Build result data.table ----
cli::cli_h2("Building output data")

result_dt <- data.table(
  delivery_id = delivery_ids,
  match_id = match_ids,
  match_date = match_dates,
  batter_id = batter_ids,
  bowler_id = bowler_ids,
  batter_run_elo_before = result_mat[, 1],
  batter_run_elo_after = result_mat[, 2],
  bowler_run_elo_before = result_mat[, 3],
  bowler_run_elo_after = result_mat[, 4],
  batter_wicket_elo_before = result_mat[, 5],
  batter_wicket_elo_after = result_mat[, 6],
  bowler_wicket_elo_before = result_mat[, 7],
  bowler_wicket_elo_after = result_mat[, 8],
  exp_runs = result_mat[, 9],
  exp_wicket = result_mat[, 10],
  actual_runs = as.integer(result_mat[, 11]),
  is_wicket = as.logical(result_mat[, 12])
)

# 4.14 Batch insert ----
cli::cli_h2("Inserting to database")

n_batches <- ceiling(n_deliveries / BATCH_SIZE)
cli::cli_progress_bar("Inserting batches", total = n_batches)

for (b in seq_len(n_batches)) {
  start_idx <- (b - 1) * BATCH_SIZE + 1
  end_idx <- min(b * BATCH_SIZE, n_deliveries)

  batch_df <- as.data.frame(result_dt[start_idx:end_idx])
  insert_format_elos(batch_df, current_category, conn)

  cli::cli_progress_update()
}

cli::cli_progress_done()

cli::cli_alert_success("Inserted {format(n_deliveries, big.mark = ',')} deliveries")

# 4.15 Store Parameters ----
cli::cli_h2("Storing parameters")

last_row <- result_dt[.N]
store_elo_params(
  current_params,
  last_delivery_id = last_row$delivery_id,
  last_match_date = last_row$match_date,
  total_deliveries = if (mode == "full") n_deliveries else stored_params$total_deliveries + n_deliveries,
  conn = conn
)

cli::cli_alert_success("Parameters stored for future incremental updates")

# 4.16 Summary Statistics ----
cat("\n")
cli::cli_h2("{toupper(current_gender)} {toupper(current_format)} ELO Summary")

stats <- get_format_elo_stats(current_category, conn)

if (!is.null(stats)) {
  cli::cli_alert_info("Total records: {format(stats$total_records, big.mark = ',')}")
  cli::cli_alert_info("Unique batters: {stats$unique_batters}")
  cli::cli_alert_info("Unique bowlers: {stats$unique_bowlers}")
  cli::cli_alert_info("Date range: {stats$first_date} to {stats$last_date}")

  cat("\nMean ELOs:\n")
  cat(sprintf("  Batter Run ELO:    %.1f\n", stats$mean_batter_run_elo))
  cat(sprintf("  Bowler Run ELO:    %.1f\n", stats$mean_bowler_run_elo))
  cat(sprintf("  Batter Wicket ELO: %.1f\n", stats$mean_batter_wicket_elo))
  cat(sprintf("  Bowler Wicket ELO: %.1f\n", stats$mean_bowler_wicket_elo))

  # Top 10 players for sense checking (with team, avg opponent ELO, and propagation delta)
  cat("\n")
  cli::cli_h3("Top 10 Batters (by Run ELO)")
  top_batters <- DBI::dbGetQuery(conn, sprintf("
    WITH latest_elo AS (
      SELECT e.batter_id,
             e.batter_run_elo_after as run_elo,
             d.batting_team,
             ROW_NUMBER() OVER (PARTITION BY e.batter_id ORDER BY e.match_date DESC, e.delivery_id DESC) as rn
      FROM %s_player_elo e
      JOIN cricsheet.deliveries d ON e.delivery_id = d.delivery_id
    ),
    avg_opp AS (
      SELECT batter_id,
             AVG(bowler_run_elo_before) as avg_opp_elo,
             COUNT(*) as balls
      FROM %s_player_elo
      GROUP BY batter_id
    )
    SELECT l.batter_id, l.run_elo, l.batting_team,
           a.avg_opp_elo, a.balls
    FROM latest_elo l
    JOIN avg_opp a ON l.batter_id = a.batter_id
    WHERE l.rn = 1
    ORDER BY l.run_elo DESC
    LIMIT 10
  ", current_category, current_category))

  cat(sprintf("  %-3s %-22s %-15s %7s %7s %6s %8s\n",
              "#", "Player", "Team", "Run ELO", "AvgOpp", "Balls", "PropAdj"))
  cat(sprintf("  %-3s %-22s %-15s %7s %7s %6s %8s\n",
              "---", "----------------------", "---------------", "-------", "-------", "------", "--------"))
  for (i in seq_len(nrow(top_batters))) {
    player <- top_batters$batter_id[i]
    team <- substr(top_batters$batting_team[i] %||% "Unknown", 1, 15)
    adj <- if (USE_PROPAGATION && exists("prop_run_adj")) prop_run_adj[[player]] %||% 0 else 0
    cat(sprintf("  %2d. %-22s %-15s %7.0f %7.0f %6d %+8.1f\n",
                i, substr(player, 1, 22), team,
                top_batters$run_elo[i],
                top_batters$avg_opp_elo[i],
                top_batters$balls[i],
                adj))
  }

  cat("\n")
  cli::cli_h3("Top 10 Bowlers (by Run ELO - higher = better at preventing runs)")
  top_bowlers <- DBI::dbGetQuery(conn, sprintf("
    WITH latest_elo AS (
      SELECT e.bowler_id,
             e.bowler_run_elo_after as run_elo,
             d.bowling_team,
             ROW_NUMBER() OVER (PARTITION BY e.bowler_id ORDER BY e.match_date DESC, e.delivery_id DESC) as rn
      FROM %s_player_elo e
      JOIN cricsheet.deliveries d ON e.delivery_id = d.delivery_id
    ),
    avg_opp AS (
      SELECT bowler_id,
             AVG(batter_run_elo_before) as avg_opp_elo,
             COUNT(*) as balls
      FROM %s_player_elo
      GROUP BY bowler_id
    )
    SELECT l.bowler_id, l.run_elo, l.bowling_team,
           a.avg_opp_elo, a.balls
    FROM latest_elo l
    JOIN avg_opp a ON l.bowler_id = a.bowler_id
    WHERE l.rn = 1
    ORDER BY l.run_elo DESC
    LIMIT 10
  ", current_category, current_category))

  cat(sprintf("  %-3s %-22s %-15s %7s %7s %6s %8s\n",
              "#", "Player", "Team", "Run ELO", "AvgOpp", "Balls", "PropAdj"))
  cat(sprintf("  %-3s %-22s %-15s %7s %7s %6s %8s\n",
              "---", "----------------------", "---------------", "-------", "-------", "------", "--------"))
  for (i in seq_len(nrow(top_bowlers))) {
    player <- top_bowlers$bowler_id[i]
    team <- substr(top_bowlers$bowling_team[i] %||% "Unknown", 1, 15)
    adj <- if (USE_PROPAGATION && exists("prop_run_adj")) prop_run_adj[[player]] %||% 0 else 0
    cat(sprintf("  %2d. %-22s %-15s %7.0f %7.0f %6d %+8.1f\n",
                i, substr(player, 1, 22), team,
                top_bowlers$run_elo[i],
                top_bowlers$avg_opp_elo[i],
                top_bowlers$balls[i],
                adj))
  }

  if (USE_DYNAMIC_K) {
    cat("\nDynamic K System:\n")
    cat(sprintf("  K-factors: Decay based on experience (halflife ~%d deliveries)\n",
                switch(current_format,
                       "t20" = PLAYER_K_RUN_HALFLIFE_T20,
                       "odi" = PLAYER_K_RUN_HALFLIFE_ODI,
                       "test" = PLAYER_K_RUN_HALFLIFE_TEST,
                       PLAYER_K_RUN_HALFLIFE_T20)))
    cat(sprintf("  Opponent K modifiers: boost=%.1f%%, reduce=%.1f%% (per 400 ELO)\n",
                PLAYER_OPP_K_BOOST * 100, PLAYER_OPP_K_REDUCE * 100))
  }

  if (USE_PROPAGATION) {
    cat("\nTeam-Level Correlation-Based Propagation:\n")
    cat(sprintf("  Teams tracked: ~%d (vs %d players)\n",
                length(all_teams), length(all_players)))
    cat(sprintf("  Propagation factor: %.0f%% of ELO change propagates to correlated teams\n",
                PLAYER_PROPAGATION_FACTOR * 100))
    cat(sprintf("  Correlation threshold: %.0f%% minimum to propagate\n",
                PLAYER_CORR_THRESHOLD * 100))
    cat(sprintf("  Weights: opponents=%.0f%%, events=%.0f%%, direct=%.0f%%\n",
                PLAYER_CORR_W_OPPONENTS * 100,
                PLAYER_CORR_W_EVENTS * 100,
                PLAYER_CORR_W_DIRECT * 100))
  }
}

# Check for drift
cat("\n")
cli::cli_h3("Drift Check")
drift_run <- abs(stats$mean_batter_run_elo - DUAL_ELO_TARGET_MEAN)
drift_wicket <- abs(stats$mean_batter_wicket_elo - DUAL_ELO_TARGET_MEAN)

if (drift_run > NORMALIZATION_THRESHOLD) {
  cli::cli_alert_warning("Run ELO drift: {round(drift_run, 1)} points from target")
} else {
  cli::cli_alert_success("Run ELO drift OK: {round(drift_run, 1)} points")
}

if (drift_wicket > NORMALIZATION_THRESHOLD) {
  cli::cli_alert_warning("Wicket ELO drift: {round(drift_wicket, 1)} points from target")
} else {
  cli::cli_alert_success("Wicket ELO drift OK: {round(drift_wicket, 1)} points")
}

# Category Done ----
cat("\n")
cli::cli_alert_success("{toupper(current_gender)} {toupper(current_format)} ELO calculation complete!")
cat("\n")

}  # End of format loop
}  # End of gender loop

# 5. Final Summary ----
cat("\n")
cli::cli_h1("All Categories Complete")
cli::cli_alert_success("Processed genders: {paste(toupper(genders_to_process), collapse = ', ')}")
cli::cli_alert_success("Processed formats: {paste(toupper(formats_to_process), collapse = ', ')}")
cat("\n")

cli::cli_h3("Next Steps")
cli::cli_bullets(c(
  "i" = "Run 04_apply_elo_normalization.R if drift exceeds threshold",
  "i" = "Run 05_validate_dual_elos.R to validate calibration",
  "i" = "ELO data stored in <gender>_<format>_player_elo tables (e.g., mens_t20_player_elo)",
  "i" = "Re-run this script for incremental updates (new matches)"
))

if (USE_DYNAMIC_K) {
  cat("\n")
  cli::cli_h3("Dynamic K-Factor System Active")
  cli::cli_bullets(c(
    "*" = "New players: Higher K-factors (faster learning, ~K_MAX)",
    "*" = "Experienced players: Lower K-factors (more stable, ~K_MIN)",
    "*" = "Beating strong opponents: K boosted (bigger ELO gain)",
    "*" = "Losing to weak opponents: K boosted (bigger ELO loss)",
    "*" = "Tier-based starting ELOs: IPL/BBL players start higher"
  ))
}

if (USE_PROPAGATION) {
  cat("\n")
  cli::cli_h3("Team-Level Correlation-Based Propagation Active")
  cli::cli_bullets(c(
    "*" = "Uses TEAM-level correlation (~500 teams vs 8000+ players)",
    "*" = "Players inherit adjustments from their primary team",
    "*" = "Teams in isolated pools (Kenya, Nepal, etc.) anchored via shared opponents",
    "*" = "Same events: Teams in same tournaments share ELO adjustments",
    "*" = "Prevents inflated ELOs from weak-vs-weak matchups"
  ))
}
cat("\n")

cli::cli_h3("Query Example")
cat("
-- Men's T20 player ELO example
SELECT d.*, e.batter_run_elo_before, e.batter_wicket_elo_before
FROM cricsheet.deliveries d
JOIN cricsheet.matches m ON d.match_id = m.match_id
JOIN mens_t20_player_elo e ON d.delivery_id = e.delivery_id
WHERE LOWER(d.match_type) IN ('t20', 'it20')
  AND m.gender = 'male'
LIMIT 10

-- Women's ODI player ELO example
SELECT d.*, e.batter_run_elo_before, e.batter_wicket_elo_before
FROM cricsheet.deliveries d
JOIN cricsheet.matches m ON d.match_id = m.match_id
JOIN womens_odi_player_elo e ON d.delivery_id = e.delivery_id
WHERE LOWER(d.match_type) IN ('odi', 'odm')
  AND m.gender = 'female'
LIMIT 10
")
cat("\n")
