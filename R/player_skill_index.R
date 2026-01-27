# Player Skill Index Functions
#
# Exponential Moving Average (EMA) based player skill tracking.
# Unlike ELO, this approach:
#   - Is drift-proof by design (indices are actual averages)
#   - Handles continuous outcomes (runs) naturally
#   - Handles rare events (wickets) without imbalance
#   - Is directly interpretable (e.g., BSI of 1.8 = 1.8 runs/ball average)


#' Get Skill Alpha for Format
#'
#' Returns the EMA alpha (learning rate) for a given format.
#'
#' @param format Character. Format: "t20", "odi", or "test"
#'
#' @return Numeric. Alpha value for EMA
#' @keywords internal
get_skill_alpha <- function(format = "t20") {
  fmt <- normalize_format(format)
  switch(fmt,
    "t20" = SKILL_ALPHA_T20,
    "odi" = SKILL_ALPHA_ODI,
    "test" = SKILL_ALPHA_TEST,
    SKILL_ALPHA_T20
  )
}


#' Get Starting Skill Values for Format
#'
#' Returns starting skill values for a format.
#' - Indices (scoring/economy): Start at 0 (neutral deviation from expected)
#' - Rates (survival/strike): Start at format average probability
#'
#' @param format Character. Format: "t20", "odi", or "test"
#'
#' @return Named list with:
#'   - scoring_index: Starting scoring index (0 = neutral)
#'   - survival_rate: Starting survival rate (format average)
#'   - economy_index: Starting economy index (0 = neutral)
#'   - strike_rate: Starting strike rate (format wicket rate)
#' @keywords internal
get_skill_start_values <- function(format = "t20") {
  fmt <- normalize_format(format)
  switch(fmt,
    "t20" = list(
      scoring_index = SKILL_START_SCORING_INDEX,
      survival_rate = SKILL_START_SURVIVAL_T20,
      economy_index = SKILL_START_ECONOMY_INDEX,
      strike_rate = SKILL_START_STRIKE_T20
    ),
    "odi" = list(
      scoring_index = SKILL_START_SCORING_INDEX,
      survival_rate = SKILL_START_SURVIVAL_ODI,
      economy_index = SKILL_START_ECONOMY_INDEX,
      strike_rate = SKILL_START_STRIKE_ODI
    ),
    "test" = list(
      scoring_index = SKILL_START_SCORING_INDEX,
      survival_rate = SKILL_START_SURVIVAL_TEST,
      economy_index = SKILL_START_ECONOMY_INDEX,
      strike_rate = SKILL_START_STRIKE_TEST
    ),
    list(
      scoring_index = SKILL_START_SCORING_INDEX,
      survival_rate = SKILL_START_SURVIVAL_T20,
      economy_index = SKILL_START_ECONOMY_INDEX,
      strike_rate = SKILL_START_STRIKE_T20
    )
  )
}


#' Update Skill Index (EMA)
#'
#' Updates a skill index using exponential moving average.
#' new_value = alpha * observation + (1 - alpha) * old_value
#'
#' @param old_value Numeric. Current skill index value
#' @param observation Numeric. New observation (runs scored, or 0/1 for wicket)
#' @param alpha Numeric. Learning rate (0-1). Higher = faster adaptation.
#'
#' @return Numeric. Updated skill index
#' @keywords internal
update_skill_index <- function(old_value, observation, alpha) {
  alpha * observation + (1 - alpha) * old_value
}


#' Calculate Expected Runs from Skill Indices
#'
#' Predicts expected runs for a batter-bowler matchup.
#'
#' @param batter_bsi Numeric. Batter Scoring Index (runs/ball average)
#' @param bowler_bei Numeric. Bowler Economy Index (runs/ball conceded average)
#' @param base_rate Numeric. Optional calibrated base rate. If NULL, uses average of indices.
#'
#' @return Numeric. Expected runs for this delivery
#' @keywords internal
calculate_expected_runs_skill <- function(batter_bsi, bowler_bei, base_rate = NULL) {
  if (is.null(base_rate)) {
    # Simple average of batter's scoring and bowler's economy
    return((batter_bsi + bowler_bei) / 2)
  }

  # Adjust base rate by difference from average
  # If batter is above average and bowler concedes above average, expect more runs
  batter_effect <- batter_bsi - base_rate
  bowler_effect <- bowler_bei - base_rate

  base_rate + (batter_effect + bowler_effect) / 2
}


#' Calculate Expected Wicket Probability from Skill Indices
#'
#' Predicts wicket probability for a batter-bowler matchup.
#'
#' @param batter_bsr Numeric. Batter Survival Rate (0-1, higher = survives more)
#' @param bowler_bsi Numeric. Bowler Strike Index (0-1, higher = takes more wickets)
#' @param base_rate Numeric. Optional calibrated base wicket rate.
#'
#' @return Numeric. Expected wicket probability for this delivery
#' @keywords internal
calculate_expected_wicket_skill <- function(batter_bsr, bowler_bsi, base_rate = NULL) {
  # batter_bsr is survival rate (e.g., 0.975 = survives 97.5% of balls)
  # bowler_bsi is strike rate (e.g., 0.03 = takes wicket on 3% of balls)

  batter_wicket_rate <- 1 - batter_bsr
  bowler_wicket_rate <- bowler_bsi

  if (is.null(base_rate)) {
    # Average of the two rates
    return((batter_wicket_rate + bowler_wicket_rate) / 2)
  }

  # Adjust base rate
  batter_effect <- batter_wicket_rate - base_rate
  bowler_effect <- bowler_wicket_rate - base_rate

  # Clamp to [0, 1]
  prob <- base_rate + (batter_effect + bowler_effect) / 2
  max(0, min(1, prob))
}


#' Create Format-Specific Skill Table
#'
#' Creates a skill index table for a specific format.
#'
#' @param format Character. Format: "t20", "odi", or "test"
#' @param conn DBI connection. Database connection
#' @param overwrite Logical. If TRUE, drops and recreates table. Default FALSE.
#'
#' @return Invisibly returns TRUE on success
#' @keywords internal
create_format_skill_table <- function(format = "t20", conn, overwrite = FALSE) {

  table_name <- paste0(format, "_player_skill")

  # Check if table exists
  tables <- DBI::dbListTables(conn)

  if (table_name %in% tables) {
    if (!overwrite) {
      cli::cli_alert_info("Table '{table_name}' already exists")
      return(invisible(TRUE))
    }
    cli::cli_alert_warning("Dropping existing '{table_name}' table")
    DBI::dbExecute(conn, paste0("DROP TABLE ", table_name))
  }

  # Create table
  cli::cli_alert_info("Creating '{table_name}' table...")

  DBI::dbExecute(conn, sprintf("
    CREATE TABLE %s (
      delivery_id VARCHAR PRIMARY KEY,
      match_id VARCHAR,
      match_date DATE,
      batter_id VARCHAR,
      bowler_id VARCHAR,

      -- Batter skill indices (before this delivery)
      batter_scoring_index DOUBLE,    -- EMA of runs per ball
      batter_survival_rate DOUBLE,    -- EMA of survival (1 - wicket rate)

      -- Bowler skill indices (before this delivery)
      bowler_economy_index DOUBLE,    -- EMA of runs conceded per ball
      bowler_strike_rate DOUBLE,      -- EMA of wickets per ball

      -- Expected values (from skill indices)
      exp_runs DOUBLE,
      exp_wicket DOUBLE,

      -- Actual outcomes
      actual_runs INTEGER,
      is_wicket BOOLEAN,

      -- Ball counts (for reliability)
      batter_balls_faced INTEGER,
      bowler_balls_bowled INTEGER
    )
  ", table_name))

  # Create indexes
  DBI::dbExecute(conn, sprintf("CREATE INDEX IF NOT EXISTS idx_%s_skill_match ON %s(match_id)", format, table_name))
  DBI::dbExecute(conn, sprintf("CREATE INDEX IF NOT EXISTS idx_%s_skill_batter ON %s(batter_id)", format, table_name))
  DBI::dbExecute(conn, sprintf("CREATE INDEX IF NOT EXISTS idx_%s_skill_bowler ON %s(bowler_id)", format, table_name))
  DBI::dbExecute(conn, sprintf("CREATE INDEX IF NOT EXISTS idx_%s_skill_date ON %s(match_date)", format, table_name))

  cli::cli_alert_success("Created '{table_name}' table with indexes")

  invisible(TRUE)
}


#' Insert Format Skill Records
#'
#' Batch inserts skill records into a format-specific table.
#'
#' @param skills_df Data frame. Skill records to insert
#' @param format Character. Format: "t20", "odi", or "test"
#' @param conn DBI connection. Database connection
#'
#' @return Invisibly returns number of rows inserted
#' @keywords internal
insert_format_skills <- function(skills_df, format = "t20", conn) {

  table_name <- paste0(format, "_player_skill")

  if (!table_name %in% DBI::dbListTables(conn)) {
    cli::cli_alert_danger("Table '{table_name}' does not exist")
    cli::cli_alert_info("Run create_format_skill_table('{format}', conn) first")
    return(invisible(0))
  }

  # Required columns
  required_cols <- c("delivery_id", "match_id", "match_date", "batter_id", "bowler_id",
                     "batter_scoring_index", "batter_survival_rate",
                     "bowler_economy_index", "bowler_strike_rate",
                     "exp_runs", "exp_wicket", "actual_runs", "is_wicket",
                     "batter_balls_faced", "bowler_balls_bowled")

  missing_cols <- setdiff(required_cols, names(skills_df))
  if (length(missing_cols) > 0) {
    cli::cli_alert_danger("Missing columns: {paste(missing_cols, collapse = ', ')}")
    return(invisible(0))
  }

  # Select only required columns
  skills_df <- skills_df[, required_cols]

  # Insert
  DBI::dbWriteTable(conn, table_name, skills_df, append = TRUE)

  invisible(nrow(skills_df))
}


#' Get Format Skill Statistics
#'
#' Retrieves summary statistics for skill indices.
#'
#' @param format Character. Format: "t20", "odi", or "test"
#' @param conn DBI connection. Database connection
#'
#' @return Data frame with skill statistics
#' @keywords internal
get_format_skill_stats <- function(format = "t20", conn) {

  table_name <- paste0(format, "_player_skill")

  if (!table_name %in% DBI::dbListTables(conn)) {
    cli::cli_alert_warning("Table '{table_name}' does not exist")
    return(NULL)
  }

  DBI::dbGetQuery(conn, sprintf("
    SELECT
      COUNT(*) as total_records,
      COUNT(DISTINCT batter_id) as unique_batters,
      COUNT(DISTINCT bowler_id) as unique_bowlers,
      MIN(match_date) as first_date,
      MAX(match_date) as last_date,
      AVG(batter_scoring_index) as mean_batter_scoring,
      AVG(bowler_economy_index) as mean_bowler_economy,
      AVG(batter_survival_rate) as mean_batter_survival,
      AVG(bowler_strike_rate) as mean_bowler_strike,
      AVG(exp_runs) as mean_exp_runs,
      AVG(actual_runs) as mean_actual_runs,
      AVG(CAST(is_wicket AS DOUBLE)) as actual_wicket_rate
    FROM %s
  ", table_name))
}


#' Get All Players' Final Skill State
#'
#' Loads the final skill indices for all players from existing data.
#' Used for incremental processing.
#'
#' @param format Character. Format: "t20", "odi", or "test"
#' @param conn DBI connection. Database connection
#'
#' @return List with environments for each skill type
#' @keywords internal
get_all_player_skill_state <- function(format = "t20", conn) {
  table_name <- get_skill_table_name(format, "player_skill")

  if (!table_name %in% DBI::dbListTables(conn)) {
    return(NULL)
  }

  # Get final skill indices for each player from their most recent appearance
  player_skills <- DBI::dbGetQuery(conn, sprintf("
    WITH all_appearances AS (
      -- Batter appearances
      SELECT
        batter_id as player_id,
        batter_scoring_index as scoring_index,
        batter_survival_rate as survival_rate,
        batter_balls_faced as balls,
        'batter' as role,
        match_date,
        delivery_id
      FROM %s

      UNION ALL

      -- Bowler appearances
      SELECT
        bowler_id as player_id,
        bowler_economy_index as scoring_index,
        bowler_strike_rate as survival_rate,
        bowler_balls_bowled as balls,
        'bowler' as role,
        match_date,
        delivery_id
      FROM %s
    ),
    ranked AS (
      SELECT
        player_id,
        role,
        scoring_index,
        survival_rate,
        balls,
        ROW_NUMBER() OVER (PARTITION BY player_id, role ORDER BY match_date DESC, delivery_id DESC) as rn
      FROM all_appearances
    )
    SELECT player_id, role, scoring_index, survival_rate, balls
    FROM ranked
    WHERE rn = 1
  ", table_name, table_name))

  if (nrow(player_skills) == 0) {
    return(NULL)
  }

  # Create environments for fast lookup
  batter_scoring <- new.env(hash = TRUE)
  batter_survival <- new.env(hash = TRUE)
  batter_balls <- new.env(hash = TRUE)
  bowler_economy <- new.env(hash = TRUE)
  bowler_strike <- new.env(hash = TRUE)
  bowler_balls <- new.env(hash = TRUE)

  # Populate from query results
  for (i in seq_len(nrow(player_skills))) {
    p <- player_skills$player_id[i]
    role <- player_skills$role[i]

    if (role == "batter") {
      batter_scoring[[p]] <- player_skills$scoring_index[i]
      batter_survival[[p]] <- player_skills$survival_rate[i]
      batter_balls[[p]] <- player_skills$balls[i]
    } else {
      bowler_economy[[p]] <- player_skills$scoring_index[i]
      bowler_strike[[p]] <- player_skills$survival_rate[i]
      bowler_balls[[p]] <- player_skills$balls[i]
    }
  }

  n_batters <- length(ls(batter_scoring))
  n_bowlers <- length(ls(bowler_economy))
  cli::cli_alert_success("Loaded skill state for {n_batters} batters and {n_bowlers} bowlers")

  list(
    batter_scoring = batter_scoring,
    batter_survival = batter_survival,
    batter_balls = batter_balls,
    bowler_economy = bowler_economy,
    bowler_strike = bowler_strike,
    bowler_balls = bowler_balls,
    n_batters = n_batters,
    n_bowlers = n_bowlers
  )
}


#' Get Last Processed Skill Delivery
#'
#' Gets information about the last processed delivery for incremental updates.
#'
#' @param format Character. Format: "t20", "odi", or "test"
#' @param conn DBI connection. Database connection
#'
#' @return Data frame with last_delivery_id and last_match_date, or NULL
#' @keywords internal
get_last_processed_skill_delivery <- function(format = "t20", conn) {
  table_name <- paste0(format, "_player_skill")

  if (!table_name %in% DBI::dbListTables(conn)) {
    return(NULL)
  }

  result <- DBI::dbGetQuery(conn, sprintf("
    SELECT delivery_id, match_date
    FROM %s
    ORDER BY match_date DESC, delivery_id DESC
    LIMIT 1
  ", table_name))

  if (nrow(result) == 0) {
    return(NULL)
  }

  result
}


#' Join Skill Indices to Deliveries
#'
#' Joins skill indices from the skill table to a deliveries data frame.
#' This is the primary way to add skill features to modelling data.
#'
#' @param deliveries_df Data frame. Must contain delivery_id column.
#' @param format Character. Format: "t20", "odi", or "test"
#' @param conn DBI connection. Database connection
#' @param skill_columns Character vector. Which skill columns to join.
#'   Default is all: batter_scoring_index, batter_survival_rate,
#'   bowler_economy_index, bowler_strike_rate, batter_balls_faced, bowler_balls_bowled
#'
#' @return Data frame with skill columns joined
#' @keywords internal
join_skill_indices <- function(deliveries_df, format = "t20", conn,
                               skill_columns = c("batter_scoring_index",
                                                "batter_survival_rate",
                                                "bowler_economy_index",
                                                "bowler_strike_rate",
                                                "batter_balls_faced",
                                                "bowler_balls_bowled")) {

  table_name <- get_skill_table_name(format, "player_skill")

  if (!table_name %in% DBI::dbListTables(conn)) {
    cli::cli_alert_warning("Table '{table_name}' does not exist")
    cli::cli_alert_info("Run 03_calculate_t20_skill_indices.R first")
    return(deliveries_df)
  }

  if (!"delivery_id" %in% names(deliveries_df)) {
    cli::cli_alert_danger("deliveries_df must contain 'delivery_id' column")
    return(deliveries_df)
  }

  # Get skill indices for matching delivery_ids
  delivery_ids <- unique(deliveries_df$delivery_id)

  # Query in batches if too many
  batch_size <- 10000
  if (length(delivery_ids) > batch_size) {
    cli::cli_alert_info("Fetching skill indices in batches...")

    all_skills <- NULL
    n_batches <- ceiling(length(delivery_ids) / batch_size)

    for (i in seq_len(n_batches)) {
      start_idx <- (i - 1) * batch_size + 1
      end_idx <- min(i * batch_size, length(delivery_ids))
      batch_ids <- delivery_ids[start_idx:end_idx]

      # Escape single quotes in delivery IDs (e.g., Durban's_Super_Giants)
      batch_ids_escaped <- escape_sql_strings(batch_ids)

      batch_skills <- DBI::dbGetQuery(conn, sprintf("
        SELECT delivery_id, %s
        FROM %s
        WHERE delivery_id IN ('%s')
      ", paste(skill_columns, collapse = ", "),
         table_name,
         paste(batch_ids_escaped, collapse = "','")))

      all_skills <- rbind(all_skills, batch_skills)

      if (i %% 10 == 0) {
        cli::cli_alert_info("Fetched {i}/{n_batches} batches...")
      }
    }

    skill_data <- all_skills
  } else {
    # Escape single quotes in delivery IDs
    delivery_ids_escaped <- escape_sql_strings(delivery_ids)

    skill_data <- DBI::dbGetQuery(conn, sprintf("
      SELECT delivery_id, %s
      FROM %s
      WHERE delivery_id IN ('%s')
    ", paste(skill_columns, collapse = ", "),
       table_name,
       paste(delivery_ids_escaped, collapse = "','")))
  }

  if (nrow(skill_data) == 0) {
    cli::cli_alert_warning("No matching skill indices found")
    return(deliveries_df)
  }

  # Join to deliveries
  result <- dplyr::left_join(deliveries_df, skill_data, by = "delivery_id")

  n_matched <- sum(!is.na(result[[skill_columns[1]]]))
  cli::cli_alert_success("Joined skill indices: {n_matched}/{nrow(deliveries_df)} deliveries matched")

  result
}


#' Get Current Player Skill Index
#'
#' Gets the most recent skill index for a player.
#'
#' @param player_id Character. Player identifier
#' @param role Character. "batter" or "bowler"
#' @param format Character. Format: "t20", "odi", or "test"
#' @param conn DBI connection. Database connection
#'
#' @return Named list with skill indices, or NULL if not found
#' @keywords internal
get_player_skill <- function(player_id, role = "batter", format = "t20", conn) {

  table_name <- paste0(format, "_player_skill")

  if (!table_name %in% DBI::dbListTables(conn)) {
    return(NULL)
  }

  # Escape player_id to prevent SQL injection
  player_id_escaped <- escape_sql_strings(player_id)

  if (role == "batter") {
    result <- DBI::dbGetQuery(conn, sprintf("
      SELECT
        batter_id as player_id,
        batter_scoring_index as scoring_index,
        batter_survival_rate as survival_rate,
        batter_balls_faced as balls,
        match_date
      FROM %s
      WHERE batter_id = '%s'
      ORDER BY match_date DESC, delivery_id DESC
      LIMIT 1
    ", table_name, player_id_escaped))
  } else {
    result <- DBI::dbGetQuery(conn, sprintf("
      SELECT
        bowler_id as player_id,
        bowler_economy_index as economy_index,
        bowler_strike_rate as strike_rate,
        bowler_balls_bowled as balls,
        match_date
      FROM %s
      WHERE bowler_id = '%s'
      ORDER BY match_date DESC, delivery_id DESC
      LIMIT 1
    ", table_name, player_id_escaped))
  }

  if (nrow(result) == 0) {
    return(NULL)
  }

  as.list(result)
}


#' Get Skill Indices for Multiple Players
#'
#' Batch fetch of current skill indices for multiple players.
#'
#' @param player_ids Character vector. Player identifiers
#' @param role Character. "batter" or "bowler"
#' @param format Character. Format: "t20", "odi", or "test"
#' @param conn DBI connection. Database connection
#'
#' @return Data frame with player_id and skill indices
#' @keywords internal
get_players_skills <- function(player_ids, role = "batter", format = "t20", conn) {

  table_name <- paste0(format, "_player_skill")

  if (!table_name %in% DBI::dbListTables(conn)) {
    return(NULL)
  }

  # Escape player_ids to prevent SQL injection
  player_ids_escaped <- escape_sql_strings(player_ids)

  if (role == "batter") {
    result <- DBI::dbGetQuery(conn, sprintf("
      WITH ranked AS (
        SELECT
          batter_id as player_id,
          batter_scoring_index as scoring_index,
          batter_survival_rate as survival_rate,
          batter_balls_faced as balls,
          match_date,
          ROW_NUMBER() OVER (PARTITION BY batter_id ORDER BY match_date DESC, delivery_id DESC) as rn
        FROM %s
        WHERE batter_id IN ('%s')
      )
      SELECT player_id, scoring_index, survival_rate, balls, match_date
      FROM ranked
      WHERE rn = 1
    ", table_name, paste(player_ids_escaped, collapse = "','")))
  } else {
    result <- DBI::dbGetQuery(conn, sprintf("
      WITH ranked AS (
        SELECT
          bowler_id as player_id,
          bowler_economy_index as economy_index,
          bowler_strike_rate as strike_rate,
          bowler_balls_bowled as balls,
          match_date,
          ROW_NUMBER() OVER (PARTITION BY bowler_id ORDER BY match_date DESC, delivery_id DESC) as rn
        FROM %s
        WHERE bowler_id IN ('%s')
      )
      SELECT player_id, economy_index, strike_rate, balls, match_date
      FROM ranked
      WHERE rn = 1
    ", table_name, paste(player_ids_escaped, collapse = "','")))
  }

  result
}


#' Add Skill Features to Model Data
#'
#' Convenience function that adds skill index features to a deliveries
#' data frame and fills missing values with starting values.
#'
#' @param deliveries_df Data frame. Must contain delivery_id column.
#' @param format Character. Format: "t20", "odi", or "test"
#' @param conn DBI connection. Database connection
#' @param fill_missing Logical. If TRUE, fills NA with starting values. Default TRUE.
#'
#' @return Data frame with skill features added
#' @keywords internal
add_skill_features <- function(deliveries_df, format = "t20", conn, fill_missing = TRUE) {

  # Join skill indices
  result <- join_skill_indices(
    deliveries_df,
    format = format,
    conn = conn
  )

  # Fill missing values with starting values
  if (fill_missing) {
    start_vals <- get_skill_start_values(format)

    result <- result %>%
      dplyr::mutate(
        batter_scoring_index = dplyr::coalesce(batter_scoring_index, start_vals$scoring_index),
        batter_survival_rate = dplyr::coalesce(batter_survival_rate, start_vals$survival_rate),
        bowler_economy_index = dplyr::coalesce(bowler_economy_index, start_vals$economy_index),
        bowler_strike_rate = dplyr::coalesce(bowler_strike_rate, start_vals$strike_rate),
        batter_balls_faced = dplyr::coalesce(batter_balls_faced, 0L),
        bowler_balls_bowled = dplyr::coalesce(bowler_balls_bowled, 0L)
      )

    n_filled <- sum(is.na(deliveries_df$batter_scoring_index) |
                    !("batter_scoring_index" %in% names(deliveries_df)))
    if (n_filled > 0) {
      cli::cli_alert_info("Filled {n_filled} missing skill values with starting defaults")
    }
  }

  result
}
