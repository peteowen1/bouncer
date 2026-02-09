# 05 Validate Dual ELOs ----
#
# This script validates the ELO system by checking:
#   1. Mean ELO drift
#   2. Expected vs actual outcome correlations
#   3. ELO distribution
#   4. Top/bottom player rankings
#
# Supports: T20, ODI, Test formats (configurable via FORMAT_FILTER)

# 1. Setup ----
library(DBI)
library(dplyr)
devtools::load_all()

cat("\n")
cli::cli_h1("ELO System Validation")
cat("\n")

# 2. Configuration ----
FORMAT_GROUPS <- list(
  t20 = c("T20", "IT20"),
  odi = c("ODI", "ODM"),
  test = c("Test", "MDM")
)

FORMAT_FILTER <- NULL  # NULL = all formats, or "t20", "odi", "test" for single format

# Determine formats to process
if (is.null(FORMAT_FILTER)) {
  formats_to_process <- names(FORMAT_GROUPS)
} else {
  formats_to_process <- FORMAT_FILTER
}

cli::cli_alert_info("Formats to validate: {paste(toupper(formats_to_process), collapse = ', ')}")

# 3. Database Connection ----
cli::cli_h2("Connecting to database")
conn <- get_db_connection(read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
cli::cli_alert_success("Connected to database")

# 4. Process Each Format ----
for (current_format in formats_to_process) {

cat("\n")
cli::cli_h1("{toupper(current_format)} ELO Validation")
cat("\n")

# Check table exists ----
table_name <- paste0(current_format, "_player_elo")
if (!table_name %in% DBI::dbListTables(conn)) {
  cli::cli_alert_warning("Table '{table_name}' does not exist - skipping")
  cli::cli_alert_info("Run 02_calculate_dual_elos.R first")
  next
}

# 4.1 Basic Statistics ----
cli::cli_h2("Basic Statistics")

stats <- get_format_elo_stats(current_format, conn)

cli::cli_alert_info("Total records: {format(stats$total_records, big.mark = ',')}")
cli::cli_alert_info("Unique batters: {stats$unique_batters}")
cli::cli_alert_info("Unique bowlers: {stats$unique_bowlers}")
cli::cli_alert_info("Date range: {stats$first_date} to {stats$last_date}")

# 4.2 Drift Check ----
cli::cli_h2("Drift Check")

target <- DUAL_ELO_TARGET_MEAN
threshold <- NORMALIZATION_THRESHOLD

cat("\nMean ELOs vs Target ({target}):\n")
cat(sprintf("  Batter Run ELO:    %.1f (drift: %+.1f)\n",
            stats$mean_batter_run_elo,
            stats$mean_batter_run_elo - target))
cat(sprintf("  Bowler Run ELO:    %.1f (drift: %+.1f)\n",
            stats$mean_bowler_run_elo,
            stats$mean_bowler_run_elo - target))
cat(sprintf("  Batter Wicket ELO: %.1f (drift: %+.1f)\n",
            stats$mean_batter_wicket_elo,
            stats$mean_batter_wicket_elo - target))
cat(sprintf("  Bowler Wicket ELO: %.1f (drift: %+.1f)\n",
            stats$mean_bowler_wicket_elo,
            stats$mean_bowler_wicket_elo - target))

# Check combined drift
run_drift <- abs((stats$mean_batter_run_elo + stats$mean_bowler_run_elo) / 2 - target)
wicket_drift <- abs((stats$mean_batter_wicket_elo + stats$mean_bowler_wicket_elo) / 2 - target)

if (run_drift > threshold || wicket_drift > threshold) {
  cli::cli_alert_warning("Drift exceeds threshold ({threshold}) - consider running normalization")
} else {
  cli::cli_alert_success("Drift within acceptable range")
}

# 4.3 ELO Distribution ----
cli::cli_h2("ELO Distribution")

distribution <- DBI::dbGetQuery(conn, sprintf("
  SELECT
    'Batter Run' as elo_type,
    MIN(batter_run_elo_after) as min_elo,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY batter_run_elo_after) as p25,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY batter_run_elo_after) as median,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY batter_run_elo_after) as p75,
    MAX(batter_run_elo_after) as max_elo,
    STDDEV(batter_run_elo_after) as std_dev
  FROM %s

  UNION ALL

  SELECT
    'Bowler Run' as elo_type,
    MIN(bowler_run_elo_after),
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY bowler_run_elo_after),
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY bowler_run_elo_after),
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY bowler_run_elo_after),
    MAX(bowler_run_elo_after),
    STDDEV(bowler_run_elo_after)
  FROM %s

  UNION ALL

  SELECT
    'Batter Wicket' as elo_type,
    MIN(batter_wicket_elo_after),
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY batter_wicket_elo_after),
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY batter_wicket_elo_after),
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY batter_wicket_elo_after),
    MAX(batter_wicket_elo_after),
    STDDEV(batter_wicket_elo_after)
  FROM %s

  UNION ALL

  SELECT
    'Bowler Wicket' as elo_type,
    MIN(bowler_wicket_elo_after),
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY bowler_wicket_elo_after),
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY bowler_wicket_elo_after),
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY bowler_wicket_elo_after),
    MAX(bowler_wicket_elo_after),
    STDDEV(bowler_wicket_elo_after)
  FROM %s
", table_name, table_name, table_name, table_name))

cat("\nELO Distribution:\n")
cat(sprintf("%-15s %8s %8s %8s %8s %8s %8s\n",
            "Type", "Min", "P25", "Median", "P75", "Max", "StdDev"))
cat(paste(rep("-", 70), collapse = ""), "\n")
for (i in seq_len(nrow(distribution))) {
  d <- distribution[i, ]
  cat(sprintf("%-15s %8.0f %8.0f %8.0f %8.0f %8.0f %8.1f\n",
              d$elo_type, d$min_elo, d$p25, d$median, d$p75, d$max_elo, d$std_dev))
}

# 4.4 Calibration Validation ----
cli::cli_h2("Calibration Validation")

# Compare expected vs actual outcomes
calibration_check <- DBI::dbGetQuery(conn, sprintf("
  SELECT
    AVG(exp_runs) as mean_exp_runs,
    AVG(CASE WHEN is_wicket THEN 0.0 ELSE actual_runs / 6.0 END) as mean_actual_run_score,
    AVG(exp_wicket) as mean_exp_wicket,
    AVG(CASE WHEN is_wicket THEN 1.0 ELSE 0.0 END) as mean_actual_wicket
  FROM %s
", table_name))

cat("\nExpected vs Actual:\n")
cat(sprintf("  Run Score:    Expected=%.4f, Actual=%.4f (diff=%+.4f)\n",
            calibration_check$mean_exp_runs,
            calibration_check$mean_actual_run_score,
            calibration_check$mean_actual_run_score - calibration_check$mean_exp_runs))
cat(sprintf("  Wicket Prob:  Expected=%.4f, Actual=%.4f (diff=%+.4f)\n",
            calibration_check$mean_exp_wicket,
            calibration_check$mean_actual_wicket,
            calibration_check$mean_actual_wicket - calibration_check$mean_exp_wicket))

# 4.5 Top Batters ----
cli::cli_h2("Top Batters by Run ELO")

top_batters <- DBI::dbGetQuery(conn, sprintf("
  WITH latest_elos AS (
    SELECT
      batter_id,
      batter_run_elo_after as run_elo,
      batter_wicket_elo_after as wicket_elo,
      ROW_NUMBER() OVER (PARTITION BY batter_id ORDER BY match_date DESC, delivery_id DESC) as rn
    FROM %s
  )
  SELECT batter_id, run_elo, wicket_elo
  FROM latest_elos
  WHERE rn = 1
  ORDER BY run_elo DESC
  LIMIT 15
", table_name))

cat("\n")
for (i in seq_len(nrow(top_batters))) {
  b <- top_batters[i, ]
  cli::cli_alert_info("{i}. {b$batter_id}: Run={round(b$run_elo)}, Wicket={round(b$wicket_elo)}")
}

# 4.6 Top Bowlers ----
cli::cli_h2("Top Bowlers by Wicket ELO")

top_bowlers <- DBI::dbGetQuery(conn, sprintf("
  WITH latest_elos AS (
    SELECT
      bowler_id,
      bowler_run_elo_after as run_elo,
      bowler_wicket_elo_after as wicket_elo,
      ROW_NUMBER() OVER (PARTITION BY bowler_id ORDER BY match_date DESC, delivery_id DESC) as rn
    FROM %s
  )
  SELECT bowler_id, run_elo, wicket_elo
  FROM latest_elos
  WHERE rn = 1
  ORDER BY wicket_elo DESC
  LIMIT 15
", table_name))

cat("\n")
for (i in seq_len(nrow(top_bowlers))) {
  b <- top_bowlers[i, ]
  cli::cli_alert_info("{i}. {b$bowler_id}: Run={round(b$run_elo)}, Wicket={round(b$wicket_elo)}")
}

# 4.7 Recent Matches Check ----
cli::cli_h2("Recent Match Sample")

recent <- DBI::dbGetQuery(conn, sprintf("
  SELECT
    match_id,
    match_date,
    COUNT(*) as n_deliveries,
    AVG(batter_run_elo_before) as avg_batter_run,
    AVG(bowler_run_elo_before) as avg_bowler_run,
    SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END) as wickets,
    SUM(actual_runs) as total_runs
  FROM %s
  GROUP BY match_id, match_date
  ORDER BY match_date DESC
  LIMIT 5
", table_name))

cat("\n")
for (i in seq_len(nrow(recent))) {
  r <- recent[i, ]
  cli::cli_alert_info("{r$match_date}: {r$n_deliveries} balls, {r$total_runs} runs, {r$wickets} wickets (avg ELO: bat={round(r$avg_batter_run)}, bowl={round(r$avg_bowler_run)})")
}

# 4.8 Summary ----
cat("\n")
cli::cli_h2("Validation Summary")

issues <- 0

if (run_drift > threshold) {
  cli::cli_alert_danger("Run ELO drift exceeds threshold")
  issues <- issues + 1
}

if (wicket_drift > threshold) {
  cli::cli_alert_danger("Wicket ELO drift exceeds threshold")
  issues <- issues + 1
}

exp_actual_diff_runs <- abs(calibration_check$mean_actual_run_score - calibration_check$mean_exp_runs)
if (exp_actual_diff_runs > 0.05) {
  cli::cli_alert_warning("Run expected/actual mismatch: {round(exp_actual_diff_runs, 4)}")
  issues <- issues + 1
}

exp_actual_diff_wicket <- abs(calibration_check$mean_actual_wicket - calibration_check$mean_exp_wicket)
if (exp_actual_diff_wicket > 0.01) {
  cli::cli_alert_warning("Wicket expected/actual mismatch: {round(exp_actual_diff_wicket, 4)}")
  issues <- issues + 1
}

if (issues == 0) {
  cli::cli_alert_success("All {toupper(current_format)} validation checks passed!")
} else {
  cli::cli_alert_warning("{toupper(current_format)}: {issues} issue(s) found - review output above")
}

}  # End of format loop

# 5. Final Summary ----
cat("\n")
cli::cli_h1("Validation Complete")
cli::cli_alert_success("Validated formats: {paste(toupper(formats_to_process), collapse = ', ')}")
cat("\n")
