# 04 Apply ELO Normalization ----
#
# This script checks for ELO drift and applies normalization if needed.
# Normalization shifts all ELOs uniformly so the mean returns to 1500.
#
# This is a SAFETY NET mechanism - the primary drift prevention is
# calibrated expected values. Only use if drift exceeds threshold.

# 1. Setup ----
library(DBI)
devtools::load_all()

cat("\n")
cli::cli_h1("ELO Normalization")
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

TARGET_MEAN <- DUAL_ELO_TARGET_MEAN
DRIFT_THRESHOLD <- NORMALIZATION_THRESHOLD

cli::cli_alert_info("Formats to check: {paste(toupper(formats_to_process), collapse = ', ')}")

# 3. Database Connection ----
cli::cli_h2("Connecting to database")
conn <- get_db_connection(read_only = FALSE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
cli::cli_alert_success("Connected to database")

# 4. Ensure normalization log table exists ----
existing_tables <- DBI::dbListTables(conn)
if (!"elo_normalization_log" %in% existing_tables) {
  cli::cli_alert_info("Creating elo_normalization_log table...")
  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS elo_normalization_log (
      normalization_id VARCHAR PRIMARY KEY,
      format VARCHAR,
      elo_type VARCHAR,
      normalization_date DATE,
      mean_before DOUBLE,
      mean_after DOUBLE,
      adjustment DOUBLE,
      players_affected INTEGER,
      created_at TIMESTAMP
    )
  ")
  cli::cli_alert_success("Created elo_normalization_log table")
}

# 5. Check Drift for Each Format ----
cli::cli_h2("Checking ELO drift")

for (format in formats_to_process) {
  table_name <- paste0(format, "_player_elo")

  if (!table_name %in% existing_tables) {
    cli::cli_alert_warning("Table '{table_name}' does not exist - skipping")
    next
  }

  cli::cli_h3("Format: {toupper(format)}")

  # Get current mean ELOs
  stats <- DBI::dbGetQuery(conn, sprintf("
    SELECT
      AVG(batter_run_elo_after) as mean_batter_run,
      AVG(bowler_run_elo_after) as mean_bowler_run,
      AVG(batter_wicket_elo_after) as mean_batter_wicket,
      AVG(bowler_wicket_elo_after) as mean_bowler_wicket,
      COUNT(DISTINCT batter_id) as n_batters,
      COUNT(DISTINCT bowler_id) as n_bowlers
    FROM %s
  ", table_name))

  # Calculate combined means
  mean_run_elo <- (stats$mean_batter_run + stats$mean_bowler_run) / 2
  mean_wicket_elo <- (stats$mean_batter_wicket + stats$mean_bowler_wicket) / 2

  cli::cli_alert_info("Run ELO mean: {round(mean_run_elo, 1)} (target: {TARGET_MEAN})")
  cli::cli_alert_info("Wicket ELO mean: {round(mean_wicket_elo, 1)} (target: {TARGET_MEAN})")

  # Check Run ELO drift
  run_drift <- mean_run_elo - TARGET_MEAN
  if (abs(run_drift) > DRIFT_THRESHOLD) {
    cli::cli_alert_warning("Run ELO drift: {round(run_drift, 1)} points - NORMALIZATION NEEDED")

    # Apply normalization
    adjustment <- -run_drift
    n_affected <- stats$n_batters + stats$n_bowlers

    DBI::dbBegin(conn)
    tryCatch({
      # Update all run ELOs
      DBI::dbExecute(conn, sprintf("
        UPDATE %s
        SET batter_run_elo_before = batter_run_elo_before + ?,
            batter_run_elo_after = batter_run_elo_after + ?,
            bowler_run_elo_before = bowler_run_elo_before + ?,
            bowler_run_elo_after = bowler_run_elo_after + ?
      ", table_name), params = list(adjustment, adjustment, adjustment, adjustment))

      # Log normalization
      log_id <- paste0(format, "_run_", format(Sys.Date(), "%Y%m%d"))
      DBI::dbExecute(conn, "
        INSERT OR REPLACE INTO elo_normalization_log
        (normalization_id, format, elo_type, normalization_date,
         mean_before, mean_after, adjustment, players_affected, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
      ", params = list(
        log_id, format, "run", as.character(Sys.Date()),
        mean_run_elo, TARGET_MEAN, adjustment, n_affected, Sys.time()
      ))

      DBI::dbCommit(conn)
      cli::cli_alert_success("Applied Run ELO adjustment: {round(adjustment, 1)} to {n_affected} player entries")

    }, error = function(e) {
      DBI::dbRollback(conn)
      cli::cli_alert_danger("Normalization failed: {e$message}")
    })

  } else {
    cli::cli_alert_success("Run ELO drift within threshold: {round(run_drift, 1)} points")
  }

  # Check Wicket ELO drift
  wicket_drift <- mean_wicket_elo - TARGET_MEAN
  if (abs(wicket_drift) > DRIFT_THRESHOLD) {
    cli::cli_alert_warning("Wicket ELO drift: {round(wicket_drift, 1)} points - NORMALIZATION NEEDED")

    # Apply normalization
    adjustment <- -wicket_drift
    n_affected <- stats$n_batters + stats$n_bowlers

    DBI::dbBegin(conn)
    tryCatch({
      # Update all wicket ELOs
      DBI::dbExecute(conn, sprintf("
        UPDATE %s
        SET batter_wicket_elo_before = batter_wicket_elo_before + ?,
            batter_wicket_elo_after = batter_wicket_elo_after + ?,
            bowler_wicket_elo_before = bowler_wicket_elo_before + ?,
            bowler_wicket_elo_after = bowler_wicket_elo_after + ?
      ", table_name), params = list(adjustment, adjustment, adjustment, adjustment))

      # Log normalization
      log_id <- paste0(format, "_wicket_", format(Sys.Date(), "%Y%m%d"))
      DBI::dbExecute(conn, "
        INSERT OR REPLACE INTO elo_normalization_log
        (normalization_id, format, elo_type, normalization_date,
         mean_before, mean_after, adjustment, players_affected, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
      ", params = list(
        log_id, format, "wicket", as.character(Sys.Date()),
        mean_wicket_elo, TARGET_MEAN, adjustment, n_affected, Sys.time()
      ))

      DBI::dbCommit(conn)
      cli::cli_alert_success("Applied Wicket ELO adjustment: {round(adjustment, 1)} to {n_affected} player entries")

    }, error = function(e) {
      DBI::dbRollback(conn)
      cli::cli_alert_danger("Normalization failed: {e$message}")
    })

  } else {
    cli::cli_alert_success("Wicket ELO drift within threshold: {round(wicket_drift, 1)} points")
  }
}

# 6. Show Yearly ELO Averages ----
cat("\n")
cli::cli_h2("Yearly ELO Averages")

for (format in formats_to_process) {
  table_name <- paste0(format, "_player_elo")

  if (!table_name %in% existing_tables) {
    next
  }

  cli::cli_h3("Format: {toupper(format)}")

  yearly_stats <- DBI::dbGetQuery(conn, sprintf("
    SELECT
      EXTRACT(YEAR FROM match_date) as year,
      COUNT(*) as n_deliveries,
      AVG(batter_run_elo_after) as avg_batter_run,
      AVG(bowler_run_elo_after) as avg_bowler_run,
      AVG(batter_wicket_elo_after) as avg_batter_wicket,
      AVG(bowler_wicket_elo_after) as avg_bowler_wicket
    FROM %s
    GROUP BY EXTRACT(YEAR FROM match_date)
    ORDER BY year
  ", table_name))

  if (nrow(yearly_stats) > 0) {
    cat("\n")
    cat(sprintf("%-6s %12s %12s %12s %12s %12s\n",
                "Year", "Deliveries", "Bat Run", "Bowl Run", "Bat Wkt", "Bowl Wkt"))
    cat(paste(rep("-", 78), collapse = ""), "\n")

    for (i in seq_len(nrow(yearly_stats))) {
      y <- yearly_stats[i, ]
      cat(sprintf("%-6.0f %12s %12.1f %12.1f %12.1f %12.1f\n",
                  y$year,
                  format(y$n_deliveries, big.mark = ","),
                  y$avg_batter_run,
                  y$avg_bowler_run,
                  y$avg_batter_wicket,
                  y$avg_bowler_wicket))
    }

    # Show drift from target by year
    cat("\n")
    cat("Drift from target (1500):\n")
    cat(sprintf("%-6s %12s %12s %12s %12s\n",
                "Year", "Bat Run", "Bowl Run", "Bat Wkt", "Bowl Wkt"))
    cat(paste(rep("-", 54), collapse = ""), "\n")

    for (i in seq_len(nrow(yearly_stats))) {
      y <- yearly_stats[i, ]
      cat(sprintf("%-6.0f %+12.1f %+12.1f %+12.1f %+12.1f\n",
                  y$year,
                  y$avg_batter_run - TARGET_MEAN,
                  y$avg_bowler_run - TARGET_MEAN,
                  y$avg_batter_wicket - TARGET_MEAN,
                  y$avg_bowler_wicket - TARGET_MEAN))
    }
  }
}

# 7. Show Yearly Skill Index Averages ----
cat("\n")
cli::cli_h2("Yearly Skill Index Averages (Drift-Proof)")

for (format in formats_to_process) {
  skill_table <- paste0(format, "_player_skill")

  if (!skill_table %in% existing_tables) {
    cli::cli_alert_warning("Skill table '{skill_table}' does not exist - run 03_calculate_skill_indices.R")
    next
  }

  cli::cli_h3("Format: {toupper(format)}")

  skill_start <- get_skill_start_values(format)

  yearly_skill <- DBI::dbGetQuery(conn, sprintf("
    SELECT
      EXTRACT(YEAR FROM match_date) as year,
      COUNT(*) as n_deliveries,
      AVG(batter_scoring_index) as avg_bat_scoring,
      AVG(bowler_economy_index) as avg_bowl_economy,
      AVG(batter_survival_rate) as avg_bat_survival,
      AVG(bowler_strike_rate) as avg_bowl_strike,
      AVG(actual_runs) as actual_runs,
      AVG(CAST(is_wicket AS DOUBLE)) as actual_wicket_rate
    FROM %s
    GROUP BY EXTRACT(YEAR FROM match_date)
    ORDER BY year
  ", skill_table))

  if (nrow(yearly_skill) > 0) {
    cat("\n")
    cat(sprintf("%-6s %12s %10s %10s %10s %10s %10s %10s\n",
                "Year", "Deliveries", "Bat Score", "Bowl Econ", "Bat Surv", "Bowl Strk", "Act Runs", "Act Wkt%"))
    cat(paste(rep("-", 96), collapse = ""), "\n")

    for (i in seq_len(nrow(yearly_skill))) {
      y <- yearly_skill[i, ]
      cat(sprintf("%-6.0f %12s %10.3f %10.3f %10.4f %10.4f %10.3f %10.2f%%\n",
                  y$year,
                  format(y$n_deliveries, big.mark = ","),
                  y$avg_bat_scoring,
                  y$avg_bowl_economy,
                  y$avg_bat_survival,
                  y$avg_bowl_strike,
                  y$actual_runs,
                  y$actual_wicket_rate * 100))
    }

    # Show deviation from starting values
    cat("\n")
    cat(sprintf("Deviation from starting values (runs: %.2f, survival: %.3f):\n",
                skill_start$runs, skill_start$survival))
    cat(sprintf("%-6s %10s %10s %10s %10s\n",
                "Year", "Bat Score", "Bowl Econ", "Bat Surv", "Bowl Strk"))
    cat(paste(rep("-", 52), collapse = ""), "\n")

    for (i in seq_len(nrow(yearly_skill))) {
      y <- yearly_skill[i, ]
      cat(sprintf("%-6.0f %+10.3f %+10.3f %+10.4f %+10.4f\n",
                  y$year,
                  y$avg_bat_scoring - skill_start$runs,
                  y$avg_bowl_economy - skill_start$runs,
                  y$avg_bat_survival - skill_start$survival,
                  y$avg_bowl_strike - (1 - skill_start$survival)))
    }

    cli::cli_alert_success("Skill indices are stable by design (no normalization needed)")
  }
}

# 8. Show Normalization History ----
cat("\n")
cli::cli_h2("Normalization History (ELO only)")

history <- DBI::dbGetQuery(conn, "
  SELECT *
  FROM elo_normalization_log
  ORDER BY normalization_date DESC
  LIMIT 10
")

if (nrow(history) > 0) {
  for (i in seq_len(nrow(history))) {
    h <- history[i, ]
    cli::cli_alert_info("{h$normalization_date}: {toupper(h$format)} {h$elo_type} adjusted by {round(h$adjustment, 1)} ({h$mean_before} -> {h$mean_after})")
  }
} else {
  cli::cli_alert_info("No normalization history found")
}

# 9. Done ----
cat("\n")
cli::cli_alert_success("Normalization check complete!")
cat("\n")
