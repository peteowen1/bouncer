# Diagnostic Script: Verify ELO and Skill Index Data Quality ----
#
# This script checks data completeness and quality for predictive modeling.
# Supports all formats (T20, ODI, Test) - set FORMAT below.
#
# Checks:
#   0. Data completeness - are all matches/deliveries covered?
#   1. Team ELO data completeness and correctness
#   2. Player ELO data in format-specific ELO table
#   3. Skill index data in format-specific skill table
#   4. Correlations between ratings and match outcomes

# 1. Setup ----
library(DBI)
library(dplyr)
devtools::load_all()

# 2. Configuration ----
FORMAT <- "t20"  # Options: "t20", "odi", "test"

# Format match type mappings
FORMAT_GROUPS <- list(
  t20 = c("t20", "it20"),
  odi = c("odi", "odm"),
  test = c("test", "mdm")
)

# Sample tournament for detailed analysis (use LIKE pattern)
SAMPLE_EVENT <- switch(FORMAT,
  t20 = "%Indian Premier League%",
  odi = "%ICC Cricket World Cup%",
  test = "%ICC World Test Championship%"
)

# Format-specific table names
player_elo_table <- paste0(FORMAT, "_player_elo")
player_skill_table <- paste0(FORMAT, "_player_skill")

# Build match type filter
format_types <- FORMAT_GROUPS[[FORMAT]]
type_filter <- paste(sprintf("'%s'", format_types), collapse = ", ")

cat("\n")
cli::cli_h1("{toupper(FORMAT)} Feature Diagnostic Report")
cat("\n")

# 3. Database Connection ----
conn <- get_db_connection(read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

# 4. Data Completeness Check ----
cli::cli_h2("Data Completeness Check ({toupper(FORMAT)} Matches)")

# Get total deliveries count for this format
format_deliveries <- DBI::dbGetQuery(conn, sprintf("
  SELECT COUNT(*) as n FROM deliveries
  WHERE LOWER(match_type) IN (%s)
", type_filter))$n

# Get total matches count for this format
format_matches <- DBI::dbGetQuery(conn, sprintf("
  SELECT
    COUNT(*) as total_matches,
    COUNT(DISTINCT match_id) as unique_matches,
    MIN(match_date) as first_date,
    MAX(match_date) as last_date
  FROM matches
  WHERE LOWER(match_type) IN (%s)
", type_filter))

cli::cli_alert_info("Total {toupper(FORMAT)} matches: {format_matches$total_matches} ({format_matches$first_date} to {format_matches$last_date})")
cli::cli_alert_info("Total {toupper(FORMAT)} deliveries: {format(format_deliveries, big.mark = ',')}")

# Check player ELO table completeness
cli::cli_h3("{player_elo_table} Table Coverage:")
if (player_elo_table %in% DBI::dbListTables(conn)) {
  elo_coverage <- DBI::dbGetQuery(conn, sprintf("
    SELECT
      COUNT(*) as elo_records,
      COUNT(DISTINCT match_id) as elo_matches,
      MIN(match_date) as first_date,
      MAX(match_date) as last_date
    FROM %s
  ", player_elo_table))

  # Find missing matches
  missing_elo_matches <- DBI::dbGetQuery(conn, sprintf("
    SELECT COUNT(DISTINCT m.match_id) as n
    FROM matches m
    LEFT JOIN %s e ON m.match_id = e.match_id
    WHERE LOWER(m.match_type) IN (%s)
      AND e.match_id IS NULL
  ", player_elo_table, type_filter))$n

  elo_pct <- round(elo_coverage$elo_records / format_deliveries * 100, 1)
  match_pct <- round(elo_coverage$elo_matches / format_matches$total_matches * 100, 1)

  cli::cli_alert_info("Records: {format(elo_coverage$elo_records, big.mark = ',')} ({elo_pct}% of deliveries)")
  cli::cli_alert_info("Matches covered: {elo_coverage$elo_matches} of {format_matches$total_matches} ({match_pct}%)")
  cli::cli_alert_info("Date range: {elo_coverage$first_date} to {elo_coverage$last_date}")

  if (missing_elo_matches > 0) {
    cli::cli_alert_warning("Missing ELO data for {missing_elo_matches} {toupper(FORMAT)} matches!")
    cli::cli_alert_info("Run: source('data-raw/elo/02_calculate_{FORMAT}_elos.R')")
  } else {
    cli::cli_alert_success("All {toupper(FORMAT)} matches have player ELO data")
  }
} else {
  cli::cli_alert_danger("{player_elo_table} table does not exist!")
  cli::cli_alert_info("Run: source('data-raw/elo/02_calculate_{FORMAT}_elos.R')")
}

# Check player skill table completeness
cli::cli_h3("{player_skill_table} Table Coverage:")
if (player_skill_table %in% DBI::dbListTables(conn)) {
  skill_coverage <- DBI::dbGetQuery(conn, sprintf("
    SELECT
      COUNT(*) as skill_records,
      COUNT(DISTINCT match_id) as skill_matches,
      MIN(match_date) as first_date,
      MAX(match_date) as last_date
    FROM %s
  ", player_skill_table))

  # Find missing matches
  missing_skill_matches <- DBI::dbGetQuery(conn, sprintf("
    SELECT COUNT(DISTINCT m.match_id) as n
    FROM matches m
    LEFT JOIN %s s ON m.match_id = s.match_id
    WHERE LOWER(m.match_type) IN (%s)
      AND s.match_id IS NULL
  ", player_skill_table, type_filter))$n

  skill_pct <- round(skill_coverage$skill_records / format_deliveries * 100, 1)
  match_pct <- round(skill_coverage$skill_matches / format_matches$total_matches * 100, 1)

  cli::cli_alert_info("Records: {format(skill_coverage$skill_records, big.mark = ',')} ({skill_pct}% of deliveries)")
  cli::cli_alert_info("Matches covered: {skill_coverage$skill_matches} of {format_matches$total_matches} ({match_pct}%)")
  cli::cli_alert_info("Date range: {skill_coverage$first_date} to {skill_coverage$last_date}")

  if (missing_skill_matches > 0) {
    cli::cli_alert_warning("Missing skill data for {missing_skill_matches} {toupper(FORMAT)} matches!")
    cli::cli_alert_info("Run: source('data-raw/elo/03_calculate_{FORMAT}_skill_indices.R')")
  } else {
    cli::cli_alert_success("All {toupper(FORMAT)} matches have player skill data")
  }
} else {
  cli::cli_alert_danger("{player_skill_table} table does not exist!")
  cli::cli_alert_info("Run: source('data-raw/elo/03_calculate_{FORMAT}_skill_indices.R')")
}

# Check team_elo completeness for this format
cli::cli_h3("team_elo Table Coverage ({toupper(FORMAT)}):")
if ("team_elo" %in% DBI::dbListTables(conn)) {
  team_elo_coverage <- DBI::dbGetQuery(conn, sprintf("
    SELECT
      COUNT(DISTINCT match_id) as team_elo_matches,
      MIN(match_date) as first_date,
      MAX(match_date) as last_date
    FROM team_elo
    WHERE LOWER(match_type) IN (%s)
  ", type_filter))

  # Find matches missing team ELO
  missing_team_elo <- DBI::dbGetQuery(conn, sprintf("
    SELECT COUNT(DISTINCT m.match_id) as n
    FROM matches m
    LEFT JOIN team_elo t ON m.match_id = t.match_id
    WHERE LOWER(m.match_type) IN (%s)
      AND t.match_id IS NULL
  ", type_filter))$n

  match_pct <- round(team_elo_coverage$team_elo_matches / format_matches$total_matches * 100, 1)

  cli::cli_alert_info("Matches covered: {team_elo_coverage$team_elo_matches} of {format_matches$total_matches} ({match_pct}%)")
  cli::cli_alert_info("Date range: {team_elo_coverage$first_date} to {team_elo_coverage$last_date}")

  if (missing_team_elo > 0) {
    cli::cli_alert_warning("Missing team ELO for {missing_team_elo} {toupper(FORMAT)} matches!")
    cli::cli_alert_info("Run: source('data-raw/predictive-modelling/01_calculate_team_elos.R')")
  } else {
    cli::cli_alert_success("All {toupper(FORMAT)} matches have team ELO data")
  }
} else {
  cli::cli_alert_danger("team_elo table does not exist!")
  cli::cli_alert_info("Run: source('data-raw/predictive-modelling/01_calculate_team_elos.R')")
}

# Check for stale data (recent matches without data)
cli::cli_h3("Checking for Recent Matches Without Data:")

# Build dynamic query based on available tables
elo_join <- if (player_elo_table %in% DBI::dbListTables(conn)) {
  sprintf("LEFT JOIN (SELECT DISTINCT match_id FROM %s) e ON m.match_id = e.match_id", player_elo_table)
} else {
  ""
}
skill_join <- if (player_skill_table %in% DBI::dbListTables(conn)) {
  sprintf("LEFT JOIN (SELECT DISTINCT match_id FROM %s) s ON m.match_id = s.match_id", player_skill_table)
} else {
  ""
}

elo_case <- if (player_elo_table %in% DBI::dbListTables(conn)) {
  "CASE WHEN e.match_id IS NULL THEN 'MISSING' ELSE 'OK' END as player_elo,"
} else {
  "'N/A' as player_elo,"
}
skill_case <- if (player_skill_table %in% DBI::dbListTables(conn)) {
  "CASE WHEN s.match_id IS NULL THEN 'MISSING' ELSE 'OK' END as player_skill,"
} else {
  "'N/A' as player_skill,"
}
elo_where <- if (player_elo_table %in% DBI::dbListTables(conn)) "e.match_id IS NULL OR" else ""
skill_where <- if (player_skill_table %in% DBI::dbListTables(conn)) "s.match_id IS NULL OR" else ""

recent_missing_query <- sprintf("
  SELECT
    m.match_id,
    m.match_date,
    m.event_name,
    m.team1,
    m.team2,
    %s
    %s
    CASE WHEN t.match_id IS NULL THEN 'MISSING' ELSE 'OK' END as team_elo
  FROM matches m
  %s
  %s
  LEFT JOIN (SELECT DISTINCT match_id FROM team_elo) t ON m.match_id = t.match_id
  WHERE LOWER(m.match_type) IN (%s)
    AND (%s %s t.match_id IS NULL)
  ORDER BY m.match_date DESC
  LIMIT 10
", elo_case, skill_case, elo_join, skill_join, type_filter, elo_where, skill_where)

recent_missing <- tryCatch(
  DBI::dbGetQuery(conn, recent_missing_query),
  error = function(e) data.frame()
)

if (nrow(recent_missing) > 0) {
  cli::cli_alert_warning("Recent {toupper(FORMAT)} matches with missing data:")
  print(recent_missing)

  cli::cli_h3("Recommended Actions:")
  if (player_elo_table %in% DBI::dbListTables(conn) && any(recent_missing$player_elo == "MISSING")) {
    cli::cli_bullets(c("!" = "Re-run player ELO: source('data-raw/elo/02_calculate_{FORMAT}_elos.R')"))
  }
  if (player_skill_table %in% DBI::dbListTables(conn) && any(recent_missing$player_skill == "MISSING")) {
    cli::cli_bullets(c("!" = "Re-run skill indices: source('data-raw/elo/03_calculate_{FORMAT}_skill_indices.R')"))
  }
  if (any(recent_missing$team_elo == "MISSING")) {
    cli::cli_bullets(c("!" = "Re-run team ELO: source('data-raw/predictive-modelling/01_calculate_team_elos.R')"))
  }
} else {
  cli::cli_alert_success("No recent {toupper(FORMAT)} matches are missing data")
}

cat("\n")

# 5. Team ELO Data ----
cli::cli_h2("Team ELO Data (team_elo table)")

team_elo_stats <- DBI::dbGetQuery(conn, sprintf("
  SELECT
    COUNT(*) as total_records,
    COUNT(DISTINCT team_id) as unique_teams,
    COUNT(DISTINCT match_id) as unique_matches,
    COUNT(elo_result) as has_result_elo,
    COUNT(elo_roster_combined) as has_roster_elo,
    AVG(elo_result) as mean_result_elo,
    STDDEV(elo_result) as sd_result_elo,
    MIN(match_date) as first_date,
    MAX(match_date) as last_date
  FROM team_elo
  WHERE LOWER(match_type) IN (%s)
", type_filter))
print(team_elo_stats)

# Check sample tournament specifically
event_team_elo <- DBI::dbGetQuery(conn, sprintf("
  SELECT
    COUNT(*) as records,
    COUNT(DISTINCT team_id) as teams,
    AVG(elo_result) as mean_elo,
    STDDEV(elo_result) as sd_elo
  FROM team_elo
  WHERE event_name LIKE '%s'
", SAMPLE_EVENT))
cli::cli_alert_info("Sample event team_elo records: {event_team_elo$records}, teams: {event_team_elo$teams}")

# 6. Player ELO in Deliveries Table ----
cli::cli_h2("Player ELO in Deliveries Table")

delivery_elo_stats <- DBI::dbGetQuery(conn, sprintf("
  SELECT
    COUNT(*) as total_deliveries,
    COUNT(batter_elo_after) as has_batter_elo,
    COUNT(bowler_elo_after) as has_bowler_elo,
    AVG(batter_elo_after) as mean_batter_elo,
    STDDEV(batter_elo_after) as sd_batter_elo,
    AVG(bowler_elo_after) as mean_bowler_elo,
    STDDEV(bowler_elo_after) as sd_bowler_elo
  FROM deliveries
  WHERE LOWER(match_type) IN (%s)
", type_filter))
print(delivery_elo_stats)

pct_with_elo <- round(delivery_elo_stats$has_batter_elo / delivery_elo_stats$total_deliveries * 100, 1)
cli::cli_alert_info("{pct_with_elo}% of {toupper(FORMAT)} deliveries have player ELO")

# Check by match type
elo_by_type <- DBI::dbGetQuery(conn, "
  SELECT
    match_type,
    COUNT(*) as deliveries,
    COUNT(batter_elo_after) as has_elo,
    ROUND(COUNT(batter_elo_after) * 100.0 / COUNT(*), 1) as pct_with_elo
  FROM deliveries
  GROUP BY match_type
  ORDER BY deliveries DESC
")
cli::cli_h3("ELO coverage by match type:")
print(elo_by_type)

# 7. Skill Index Table ----
cli::cli_h2("Skill Index Table ({player_skill_table})")

if (player_skill_table %in% DBI::dbListTables(conn)) {
  skill_stats <- DBI::dbGetQuery(conn, sprintf("
    SELECT
      COUNT(*) as total_records,
      COUNT(DISTINCT batter_id) as unique_batters,
      COUNT(DISTINCT bowler_id) as unique_bowlers,
      MIN(match_date) as first_date,
      MAX(match_date) as last_date,
      AVG(batter_scoring_index) as mean_batter_bsi,
      STDDEV(batter_scoring_index) as sd_batter_bsi,
      AVG(batter_survival_rate) as mean_batter_surv,
      AVG(bowler_economy_index) as mean_bowler_econ,
      STDDEV(bowler_economy_index) as sd_bowler_econ,
      AVG(bowler_strike_rate) as mean_bowler_strike
    FROM %s
  ", player_skill_table))
  print(skill_stats)

  # Check if skill data covers sample event matches
  skill_event <- DBI::dbGetQuery(conn, sprintf("
    SELECT COUNT(DISTINCT s.match_id) as skill_matches
    FROM %s s
    JOIN matches m ON s.match_id = m.match_id
    WHERE m.event_name LIKE '%s'
  ", player_skill_table, SAMPLE_EVENT))
  cli::cli_alert_info("Sample event matches with skill data: {skill_event$skill_matches}")
} else {
  cli::cli_alert_danger("{player_skill_table} table does not exist!")
}

# 8. Dual ELO Table ----
cli::cli_h2("Dual ELO Table ({player_elo_table})")

if (player_elo_table %in% DBI::dbListTables(conn)) {
  dual_elo_stats <- DBI::dbGetQuery(conn, sprintf("
    SELECT
      COUNT(*) as total_records,
      COUNT(DISTINCT batter_id) as unique_batters,
      COUNT(DISTINCT bowler_id) as unique_bowlers,
      MIN(match_date) as first_date,
      MAX(match_date) as last_date,
      AVG(batter_run_elo_after) as mean_batter_run_elo,
      STDDEV(batter_run_elo_after) as sd_batter_run_elo,
      AVG(batter_wicket_elo_after) as mean_batter_wicket_elo,
      AVG(bowler_run_elo_after) as mean_bowler_run_elo,
      AVG(bowler_wicket_elo_after) as mean_bowler_wicket_elo
    FROM %s
  ", player_elo_table))
  print(dual_elo_stats)
} else {
  cli::cli_alert_warning("{player_elo_table} table does not exist")
}

# 9. Match-Level Correlation Analysis ----
cli::cli_h2("Match-Level Correlation with Winning")

# Get matches from sample event with outcomes and team ELOs
match_data <- DBI::dbGetQuery(conn, sprintf("
  SELECT
    m.match_id,
    m.match_date,
    m.team1,
    m.team2,
    m.outcome_winner,
    t1.elo_result as team1_elo_result,
    t1.elo_roster_combined as team1_elo_roster,
    t2.elo_result as team2_elo_result,
    t2.elo_roster_combined as team2_elo_roster
  FROM matches m
  LEFT JOIN team_elo t1 ON m.match_id = t1.match_id AND m.team1 = t1.team_id
  LEFT JOIN team_elo t2 ON m.match_id = t2.match_id AND m.team2 = t2.team_id
  WHERE m.event_name LIKE '%s'
    AND m.outcome_winner IS NOT NULL
    AND m.outcome_winner != ''
  ORDER BY m.match_date
", SAMPLE_EVENT))

cli::cli_alert_info("Sample event matches with outcomes: {nrow(match_data)}")
cli::cli_alert_info("Matches with team1 ELO: {sum(!is.na(match_data$team1_elo_result))}")
cli::cli_alert_info("Matches with team1 roster ELO: {sum(!is.na(match_data$team1_elo_roster))}")

if (nrow(match_data) > 0 && sum(!is.na(match_data$team1_elo_result)) > 50) {
  match_data <- match_data %>%
    mutate(
      team1_wins = as.integer(outcome_winner == team1),
      elo_diff_result = team1_elo_result - team2_elo_result,
      elo_diff_roster = team1_elo_roster - team2_elo_roster
    ) %>%
    filter(!is.na(elo_diff_result))

  cli::cli_h3("Correlations with team1_wins:")

  cor_result <- cor(match_data$elo_diff_result, match_data$team1_wins, use = "complete.obs")
  cor_roster <- cor(match_data$elo_diff_roster, match_data$team1_wins, use = "complete.obs")

  cli::cli_alert_info("Result ELO diff correlation: {round(cor_result, 4)}")
  cli::cli_alert_info("Roster ELO diff correlation: {round(cor_roster, 4)}")

  # Prediction accuracy using ELO
  match_data$pred_result <- as.integer(match_data$elo_diff_result > 0)
  match_data$pred_roster <- as.integer(match_data$elo_diff_roster > 0)

  acc_result <- mean(match_data$pred_result == match_data$team1_wins, na.rm = TRUE)
  acc_roster <- mean(match_data$pred_roster == match_data$team1_wins, na.rm = TRUE)

  cli::cli_alert_info("Result ELO prediction accuracy: {round(acc_result * 100, 1)}%")
  cli::cli_alert_info("Roster ELO prediction accuracy: {round(acc_roster * 100, 1)}%")

  # Check by ELO difference magnitude
  cli::cli_h3("Accuracy by ELO difference magnitude:")
  match_data$elo_bucket <- cut(abs(match_data$elo_diff_result),
                                breaks = c(0, 25, 50, 100, 200, Inf),
                                labels = c("0-25", "25-50", "50-100", "100-200", "200+"))

  by_bucket <- match_data %>%
    filter(!is.na(elo_bucket)) %>%
    group_by(elo_bucket) %>%
    summarise(
      n = n(),
      accuracy = mean(pred_result == team1_wins, na.rm = TRUE),
      .groups = "drop"
    )
  print(by_bucket)
}

# 10. Skill Index Correlation Analysis ----
cli::cli_h2("Skill Index Correlation Analysis")

# Load the pre-calculated features
features_path <- file.path("..", "bouncerdata", "models", paste0(FORMAT, "_prediction_features.rds"))
if (file.exists(features_path)) {
  features_data <- readRDS(features_path)
  train_data <- features_data$train

  cli::cli_h3("Feature correlations with team1_wins (training data):")

  # Numeric columns to check
  numeric_cols <- c(
    "team1_elo_result", "team1_elo_roster",
    "team2_elo_result", "team2_elo_roster",
    "team1_bat_scoring_avg", "team1_bat_scoring_top5",
    "team1_bat_survival_avg",
    "team1_bowl_economy_avg", "team1_bowl_economy_top5",
    "team1_bowl_strike_avg",
    "team2_bat_scoring_avg", "team2_bat_scoring_top5",
    "team2_bat_survival_avg",
    "team2_bowl_economy_avg", "team2_bowl_economy_top5",
    "team2_bowl_strike_avg"
  )

  correlations <- sapply(numeric_cols, function(col) {
    if (col %in% names(train_data)) {
      cor(train_data[[col]], train_data$team1_wins, use = "complete.obs")
    } else {
      NA
    }
  })

  cor_df <- data.frame(
    feature = names(correlations),
    correlation = round(correlations, 4)
  ) %>%
    arrange(desc(abs(correlation)))

  print(cor_df)

  # Difference features
  cli::cli_h3("Difference feature correlations:")
  train_data <- train_data %>%
    mutate(
      elo_diff_result = team1_elo_result - team2_elo_result,
      elo_diff_roster = team1_elo_roster - team2_elo_roster,
      bat_scoring_diff = team1_bat_scoring_avg - team2_bat_scoring_avg,
      bat_top5_diff = team1_bat_scoring_top5 - team2_bat_scoring_top5,
      bowl_economy_diff = team2_bowl_economy_avg - team1_bowl_economy_avg,  # reversed
      bowl_strike_diff = team1_bowl_strike_avg - team2_bowl_strike_avg
    )

  diff_cols <- c("elo_diff_result", "elo_diff_roster",
                 "bat_scoring_diff", "bat_top5_diff",
                 "bowl_economy_diff", "bowl_strike_diff")

  diff_cors <- sapply(diff_cols, function(col) {
    cor(train_data[[col]], train_data$team1_wins, use = "complete.obs")
  })

  diff_df <- data.frame(
    feature = names(diff_cors),
    correlation = round(diff_cors, 4)
  ) %>%
    arrange(desc(abs(correlation)))

  print(diff_df)

} else {
  cli::cli_alert_warning("Feature file not found: {features_path}")
}

# 11. Sample Data Inspection ----
cli::cli_h2("Sample Data Inspection")

# Check a few recent matches from sample event
sample_matches <- DBI::dbGetQuery(conn, sprintf("
  SELECT
    m.match_id,
    m.match_date,
    m.team1,
    m.team2,
    m.outcome_winner,
    t1.elo_result as t1_elo,
    t2.elo_result as t2_elo
  FROM matches m
  LEFT JOIN team_elo t1 ON m.match_id = t1.match_id AND m.team1 = t1.team_id
  LEFT JOIN team_elo t2 ON m.match_id = t2.match_id AND m.team2 = t2.team_id
  WHERE m.event_name LIKE '%s'
    AND m.outcome_winner IS NOT NULL
  ORDER BY m.match_date DESC
  LIMIT 10
", SAMPLE_EVENT))

cli::cli_h3("Recent matches with team ELOs:")
print(sample_matches)

# Check for matches WITHOUT team ELO
missing_elo <- DBI::dbGetQuery(conn, sprintf("
  SELECT COUNT(*) as n
  FROM matches m
  LEFT JOIN team_elo t1 ON m.match_id = t1.match_id AND m.team1 = t1.team_id
  WHERE m.event_name LIKE '%s'
    AND t1.elo_result IS NULL
", SAMPLE_EVENT))
cli::cli_alert_info("Sample event matches missing team ELO: {missing_elo$n}")

cat("\n")
cli::cli_h1("Diagnostic Complete")
cat("\n")
