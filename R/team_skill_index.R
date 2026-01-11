# Team Skill Index Functions
#
# Per-delivery team skill tracking using residual-based updates.
# Similar to player skill indices, but tracks team-level performance.
#
# Team skills represent "how much better/worse the team performs than context-expected"
# Using the agnostic model as baseline (context-only, no player/team/venue identity).
#
# Team Skill Indices:
#   - Team Runs Skill (Batting): Residual-based scoring ability
#   - Team Wicket Skill (Batting): Residual-based survival ability (avoiding wickets)
#   - Team Runs Skill (Bowling): Residual-based economy (restricting runs)
#   - Team Wicket Skill (Bowling): Residual-based strike ability (taking wickets)


#' Get Team Skill Alpha for Format
#'
#' Returns the EMA alpha (learning rate) for team skills for a given format.
#' Team skills use a slightly lower alpha than player skills because team
#' composition changes more slowly.
#'
#' @param format Character. Format: "t20", "odi", or "test"
#'
#' @return Numeric. Alpha value for team skill updates
#' @keywords internal
get_team_skill_alpha <- function(format = "t20") {
  # Use slightly lower alpha than player skills (teams change composition slowly)
  fmt <- normalize_format(format)
  switch(fmt,
    "t20" = SKILL_ALPHA_T20 * 0.5,
    "odi" = SKILL_ALPHA_ODI * 0.5,
    "test" = SKILL_ALPHA_TEST * 0.5,
    SKILL_ALPHA_T20 * 0.5
  )
}


#' Get Starting Team Skill Values
#'
#' Returns starting team skill index values.
#' Team skills start at 0 (neutral - no deviation from context-expected).
#'
#' @param format Character. Format: "t20", "odi", or "test"
#'
#' @return Named list with start_runs and start_wicket (both 0 for residual-based)
#' @keywords internal
get_team_skill_start_values <- function(format = "t20") {
  # For residual-based indices, starting value is 0 (no deviation from expected)
  list(
    runs_skill = 0.0,     # 0 = performs exactly as context-expected
    wicket_skill = 0.0    # 0 = takes/loses wickets exactly as context-expected
  )
}


#' Update Team Skill Index (Residual-Based EMA)
#'
#' Updates a team skill index using proper EMA on residuals.
#' Formula: new = (1 - alpha) * old + alpha * residual
#'
#' This converges to the team's true deviation from expected (bounded and stable).
#'
#' @param old_skill Numeric. Current skill index value
#' @param actual Numeric. Actual outcome (runs or wicket)
#' @param expected Numeric. Agnostic model expected value
#' @param alpha Numeric. Learning rate (0-1). Higher = faster adaptation.
#'
#' @return Numeric. Updated skill index
#' @keywords internal
update_team_skill <- function(old_skill, actual, expected, alpha) {
  residual <- actual - expected
  (1 - alpha) * old_skill + alpha * residual
}


#' Create Format-Specific Team Skill Table
#'
#' Creates a team skill index table for a specific format.
#'
#' @param format Character. Format: "t20", "odi", or "test"
#' @param conn DBI connection. Database connection
#' @param overwrite Logical. If TRUE, drops and recreates table. Default FALSE.
#'
#' @return Invisibly returns TRUE on success
#' @keywords internal
create_format_team_skill_table <- function(format = "t20", conn, overwrite = FALSE) {

  table_name <- paste0(format, "_team_skill")

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
      batting_team_id VARCHAR,
      bowling_team_id VARCHAR,

      -- Batting team skill indices (before this delivery)
      batting_team_runs_skill DOUBLE,     -- Residual-based scoring ability
      batting_team_wicket_skill DOUBLE,   -- Residual-based survival ability

      -- Bowling team skill indices (before this delivery)
      bowling_team_runs_skill DOUBLE,     -- Residual-based economy (restricting runs)
      bowling_team_wicket_skill DOUBLE,   -- Residual-based strike ability

      -- Agnostic model expectations (context-only baseline)
      exp_runs_agnostic DOUBLE,
      exp_wicket_agnostic DOUBLE,

      -- Actual outcomes
      actual_runs INTEGER,
      is_wicket BOOLEAN,

      -- Ball counts (for reliability)
      batting_team_balls INTEGER,
      bowling_team_balls INTEGER
    )
  ", table_name))

  # Create indexes
  DBI::dbExecute(conn, sprintf("CREATE INDEX IF NOT EXISTS idx_%s_team_skill_match ON %s(match_id)", format, table_name))
  DBI::dbExecute(conn, sprintf("CREATE INDEX IF NOT EXISTS idx_%s_team_skill_batting ON %s(batting_team_id)", format, table_name))
  DBI::dbExecute(conn, sprintf("CREATE INDEX IF NOT EXISTS idx_%s_team_skill_bowling ON %s(bowling_team_id)", format, table_name))
  DBI::dbExecute(conn, sprintf("CREATE INDEX IF NOT EXISTS idx_%s_team_skill_date ON %s(match_date)", format, table_name))

  cli::cli_alert_success("Created '{table_name}' table with indexes")

  invisible(TRUE)
}


#' Insert Format Team Skill Records
#'
#' Batch inserts team skill records into a format-specific table.
#'
#' @param skills_df Data frame. Team skill records to insert
#' @param format Character. Format: "t20", "odi", or "test"
#' @param conn DBI connection. Database connection
#'
#' @return Invisibly returns number of rows inserted
#' @keywords internal
insert_format_team_skills <- function(skills_df, format = "t20", conn) {

  table_name <- paste0(format, "_team_skill")

  if (!table_name %in% DBI::dbListTables(conn)) {
    cli::cli_alert_danger("Table '{table_name}' does not exist")
    cli::cli_alert_info("Run create_format_team_skill_table('{format}', conn) first")
    return(invisible(0))
  }

  # Required columns
  required_cols <- c("delivery_id", "match_id", "match_date",
                     "batting_team_id", "bowling_team_id",
                     "batting_team_runs_skill", "batting_team_wicket_skill",
                     "bowling_team_runs_skill", "bowling_team_wicket_skill",
                     "exp_runs_agnostic", "exp_wicket_agnostic",
                     "actual_runs", "is_wicket",
                     "batting_team_balls", "bowling_team_balls")

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


#' Get Format Team Skill Statistics
#'
#' Retrieves summary statistics for team skill indices.
#'
#' @param format Character. Format: "t20", "odi", or "test"
#' @param conn DBI connection. Database connection
#'
#' @return Data frame with team skill statistics
#' @keywords internal
get_format_team_skill_stats <- function(format = "t20", conn) {

  table_name <- paste0(format, "_team_skill")

  if (!table_name %in% DBI::dbListTables(conn)) {
    cli::cli_alert_warning("Table '{table_name}' does not exist")
    return(NULL)
  }

  DBI::dbGetQuery(conn, sprintf("
    SELECT
      COUNT(*) as total_records,
      COUNT(DISTINCT batting_team_id) as unique_batting_teams,
      COUNT(DISTINCT bowling_team_id) as unique_bowling_teams,
      MIN(match_date) as first_date,
      MAX(match_date) as last_date,
      AVG(batting_team_runs_skill) as mean_batting_runs_skill,
      AVG(batting_team_wicket_skill) as mean_batting_wicket_skill,
      AVG(bowling_team_runs_skill) as mean_bowling_runs_skill,
      AVG(bowling_team_wicket_skill) as mean_bowling_wicket_skill,
      AVG(exp_runs_agnostic) as mean_exp_runs,
      AVG(actual_runs) as mean_actual_runs,
      AVG(CAST(is_wicket AS DOUBLE)) as actual_wicket_rate
    FROM %s
  ", table_name))
}


#' Get All Teams' Final Skill State
#'
#' Loads the final skill indices for all teams from existing data.
#' Used for incremental processing.
#'
#' @param format Character. Format: "t20", "odi", or "test"
#' @param conn DBI connection. Database connection
#'
#' @return List with environments for each skill type
#' @keywords internal
get_all_team_skill_state <- function(format = "t20", conn) {
  table_name <- get_skill_table_name(format, "team_skill")

  if (!table_name %in% DBI::dbListTables(conn)) {
    return(NULL)
  }

  # Get final skill indices for each team from their most recent appearance
  team_skills <- DBI::dbGetQuery(conn, sprintf("
    WITH all_appearances AS (
      -- Batting appearances
      SELECT
        batting_team_id as team_id,
        batting_team_runs_skill as runs_skill,
        batting_team_wicket_skill as wicket_skill,
        batting_team_balls as balls,
        'batting' as role,
        match_date,
        delivery_id
      FROM %s

      UNION ALL

      -- Bowling appearances
      SELECT
        bowling_team_id as team_id,
        bowling_team_runs_skill as runs_skill,
        bowling_team_wicket_skill as wicket_skill,
        bowling_team_balls as balls,
        'bowling' as role,
        match_date,
        delivery_id
      FROM %s
    ),
    ranked AS (
      SELECT
        team_id,
        role,
        runs_skill,
        wicket_skill,
        balls,
        ROW_NUMBER() OVER (PARTITION BY team_id, role ORDER BY match_date DESC, delivery_id DESC) as rn
      FROM all_appearances
    )
    SELECT team_id, role, runs_skill, wicket_skill, balls
    FROM ranked
    WHERE rn = 1
  ", table_name, table_name))

  if (nrow(team_skills) == 0) {
    return(NULL)
  }

  # Create environments for fast lookup
  batting_runs_skill <- new.env(hash = TRUE)
  batting_wicket_skill <- new.env(hash = TRUE)
  batting_balls <- new.env(hash = TRUE)
  bowling_runs_skill <- new.env(hash = TRUE)
  bowling_wicket_skill <- new.env(hash = TRUE)
  bowling_balls <- new.env(hash = TRUE)

  # Populate from query results
  for (i in seq_len(nrow(team_skills))) {
    t <- team_skills$team_id[i]
    role <- team_skills$role[i]

    if (role == "batting") {
      batting_runs_skill[[t]] <- team_skills$runs_skill[i]
      batting_wicket_skill[[t]] <- team_skills$wicket_skill[i]
      batting_balls[[t]] <- team_skills$balls[i]
    } else {
      bowling_runs_skill[[t]] <- team_skills$runs_skill[i]
      bowling_wicket_skill[[t]] <- team_skills$wicket_skill[i]
      bowling_balls[[t]] <- team_skills$balls[i]
    }
  }

  n_batting_teams <- length(ls(batting_runs_skill))
  n_bowling_teams <- length(ls(bowling_runs_skill))
  cli::cli_alert_success("Loaded skill state for {n_batting_teams} batting teams and {n_bowling_teams} bowling teams")

  list(
    batting_runs_skill = batting_runs_skill,
    batting_wicket_skill = batting_wicket_skill,
    batting_balls = batting_balls,
    bowling_runs_skill = bowling_runs_skill,
    bowling_wicket_skill = bowling_wicket_skill,
    bowling_balls = bowling_balls,
    n_batting_teams = n_batting_teams,
    n_bowling_teams = n_bowling_teams
  )
}


#' Get Last Processed Team Skill Delivery
#'
#' Gets information about the last processed delivery for incremental updates.
#'
#' @param format Character. Format: "t20", "odi", or "test"
#' @param conn DBI connection. Database connection
#'
#' @return Data frame with last_delivery_id and last_match_date, or NULL
#' @keywords internal
get_last_processed_team_skill_delivery <- function(format = "t20", conn) {
  table_name <- paste0(format, "_team_skill")

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


#' Get Current Team Skill Index
#'
#' Gets the most recent skill index for a team.
#'
#' @param team_id Character. Team identifier
#' @param role Character. "batting" or "bowling"
#' @param format Character. Format: "t20", "odi", or "test"
#' @param conn DBI connection. Database connection
#'
#' @return Named list with skill indices, or NULL if not found
#' @keywords internal
get_team_skill <- function(team_id, role = "batting", format = "t20", conn) {

  table_name <- paste0(format, "_team_skill")

  if (!table_name %in% DBI::dbListTables(conn)) {
    return(NULL)
  }

  if (role == "batting") {
    result <- DBI::dbGetQuery(conn, sprintf("
      SELECT
        batting_team_id as team_id,
        batting_team_runs_skill as runs_skill,
        batting_team_wicket_skill as wicket_skill,
        batting_team_balls as balls,
        match_date
      FROM %s
      WHERE batting_team_id = '%s'
      ORDER BY match_date DESC, delivery_id DESC
      LIMIT 1
    ", table_name, team_id))
  } else {
    result <- DBI::dbGetQuery(conn, sprintf("
      SELECT
        bowling_team_id as team_id,
        bowling_team_runs_skill as runs_skill,
        bowling_team_wicket_skill as wicket_skill,
        bowling_team_balls as balls,
        match_date
      FROM %s
      WHERE bowling_team_id = '%s'
      ORDER BY match_date DESC, delivery_id DESC
      LIMIT 1
    ", table_name, team_id))
  }

  if (nrow(result) == 0) {
    return(NULL)
  }

  as.list(result)
}


#' Join Team Skill Indices to Deliveries
#'
#' Joins team skill indices from the team skill table to a deliveries data frame.
#'
#' @param deliveries_df Data frame. Must contain delivery_id column.
#' @param format Character. Format: "t20", "odi", or "test"
#' @param conn DBI connection. Database connection
#' @param skill_columns Character vector. Which skill columns to join.
#'
#' @return Data frame with team skill columns joined
#' @keywords internal
join_team_skill_indices <- function(deliveries_df, format = "t20", conn,
                                     skill_columns = c("batting_team_runs_skill",
                                                       "batting_team_wicket_skill",
                                                       "bowling_team_runs_skill",
                                                       "bowling_team_wicket_skill",
                                                       "batting_team_balls",
                                                       "bowling_team_balls")) {

  table_name <- get_skill_table_name(format, "team_skill")

  if (!table_name %in% DBI::dbListTables(conn)) {
    cli::cli_alert_warning("Table '{table_name}' does not exist")
    cli::cli_alert_info("Run 02_calculate_team_skill_indices.R first")
    return(deliveries_df)
  }

  if (!"delivery_id" %in% names(deliveries_df)) {
    cli::cli_alert_danger("deliveries_df must contain 'delivery_id' column")
    return(deliveries_df)
  }

  # Get skill indices for matching delivery_ids
  delivery_ids <- unique(deliveries_df$delivery_id)

  # Escape single quotes in delivery IDs (e.g., Durban's_Super_Giants)
  delivery_ids_escaped <- escape_sql_strings(delivery_ids)

  skill_data <- DBI::dbGetQuery(conn, sprintf("
    SELECT delivery_id, %s
    FROM %s
    WHERE delivery_id IN ('%s')
  ", paste(skill_columns, collapse = ", "),
     table_name,
     paste(delivery_ids_escaped, collapse = "','")))

  if (nrow(skill_data) == 0) {
    cli::cli_alert_warning("No matching team skill indices found")
    return(deliveries_df)
  }

  # Join to deliveries
  result <- dplyr::left_join(deliveries_df, skill_data, by = "delivery_id")

  n_matched <- sum(!is.na(result[[skill_columns[1]]]))
  cli::cli_alert_success("Joined team skill indices: {n_matched}/{nrow(deliveries_df)} deliveries matched")

  result
}


#' Add Team Skill Features to Model Data
#'
#' Convenience function that adds team skill index features to a deliveries
#' data frame and fills missing values with starting values (0).
#'
#' @param deliveries_df Data frame. Must contain delivery_id column.
#' @param format Character. Format: "t20", "odi", or "test"
#' @param conn DBI connection. Database connection
#' @param fill_missing Logical. If TRUE, fills NA with 0 (neutral). Default TRUE.
#'
#' @return Data frame with team skill features added
#' @keywords internal
add_team_skill_features <- function(deliveries_df, format = "t20", conn, fill_missing = TRUE) {

  # Join team skill indices
  result <- join_team_skill_indices(
    deliveries_df,
    format = format,
    conn = conn
  )

  # Fill missing values with starting values (0 = neutral)
  if (fill_missing) {
    start_vals <- get_team_skill_start_values(format)

    result <- result %>%
      dplyr::mutate(
        batting_team_runs_skill = dplyr::coalesce(batting_team_runs_skill, start_vals$runs_skill),
        batting_team_wicket_skill = dplyr::coalesce(batting_team_wicket_skill, start_vals$wicket_skill),
        bowling_team_runs_skill = dplyr::coalesce(bowling_team_runs_skill, start_vals$runs_skill),
        bowling_team_wicket_skill = dplyr::coalesce(bowling_team_wicket_skill, start_vals$wicket_skill),
        batting_team_balls = dplyr::coalesce(batting_team_balls, 0L),
        bowling_team_balls = dplyr::coalesce(bowling_team_balls, 0L)
      )
  }

  result
}
