# 06 Validate Skill Indices ----
#
# This script provides comprehensive validation of the skill index system:
# - Yearly averages to check for drift
# - Top/Bottom players by each index
# - Distribution analysis
# - Correlation with actual outcomes
# - Player trajectory examples
#
# Supports: T20, ODI, Test formats (configurable via FORMAT_FILTER)

# 1. Setup ----
library(DBI)
library(dplyr)
devtools::load_all()

cat("\n")
cli::cli_h1("Skill Index Validation")
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
conn <- get_db_connection(read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

# 4. Process Each Format ----
for (current_format in formats_to_process) {

cat("\n")
cli::cli_h1("{toupper(current_format)} Skill Index Validation")
cat("\n")

skill_table <- paste0(current_format, "_player_skill")

# Check table exists
if (!skill_table %in% DBI::dbListTables(conn)) {
  cli::cli_alert_warning("Table '{skill_table}' does not exist - skipping")
  cli::cli_alert_info("Run 03_calculate_skill_indices.R first")
  next
}

# Get starting values for comparison
skill_start <- get_skill_start_values(current_format)
START_RUNS <- skill_start$runs
START_SURVIVAL <- skill_start$survival

# 4.1 Yearly Drift Check ----
cli::cli_h2("Yearly Drift Check")

yearly_stats <- DBI::dbGetQuery(conn, sprintf("
  SELECT
    EXTRACT(YEAR FROM match_date) as year,
    COUNT(*) as n_deliveries,
    COUNT(DISTINCT match_id) as n_matches,
    COUNT(DISTINCT batter_id) as n_batters,
    COUNT(DISTINCT bowler_id) as n_bowlers,

    -- Scoring indices
    AVG(batter_scoring_index) as avg_batter_scoring,
    AVG(bowler_economy_index) as avg_bowler_economy,
    STDDEV(batter_scoring_index) as sd_batter_scoring,
    STDDEV(bowler_economy_index) as sd_bowler_economy,

    -- Survival/Strike indices
    AVG(batter_survival_rate) as avg_batter_survival,
    AVG(bowler_strike_rate) as avg_bowler_strike,
    STDDEV(batter_survival_rate) as sd_batter_survival,
    STDDEV(bowler_strike_rate) as sd_bowler_strike,

    -- Actual outcomes for comparison
    AVG(actual_runs) as actual_avg_runs,
    AVG(CAST(is_wicket AS DOUBLE)) * 100 as actual_wicket_pct,

    -- Expected values (from model)
    AVG(exp_runs) as avg_exp_runs,
    AVG(exp_wicket) * 100 as avg_exp_wicket_pct
  FROM %s
  GROUP BY EXTRACT(YEAR FROM match_date)
  ORDER BY year
", skill_table))

cat("\n")
cat("=== Scoring Indices by Year ===\n")
cat(sprintf("%-6s %10s %10s %12s %12s %10s\n",
            "Year", "Deliveries", "Matches", "Bat Score", "Bowl Econ", "Actual Runs"))
cat(paste(rep("-", 70), collapse = ""), "\n")

for (i in seq_len(nrow(yearly_stats))) {
  y <- yearly_stats[i, ]
  cat(sprintf("%-6.0f %10s %10s %12.3f %12.3f %10.3f\n",
              y$year,
              format(y$n_deliveries, big.mark = ","),
              format(y$n_matches, big.mark = ","),
              y$avg_batter_scoring,
              y$avg_bowler_economy,
              y$actual_avg_runs))
}

cat("\n")
cat("=== Survival/Strike Indices by Year ===\n")
cat(sprintf("%-6s %12s %12s %12s %12s\n",
            "Year", "Bat Survival", "Bowl Strike", "Actual Wkt%", "Exp Wkt%"))
cat(paste(rep("-", 62), collapse = ""), "\n")

for (i in seq_len(nrow(yearly_stats))) {
  y <- yearly_stats[i, ]
  cat(sprintf("%-6.0f %12.4f %12.4f %12.2f%% %12.2f%%\n",
              y$year,
              y$avg_batter_survival,
              y$avg_bowler_strike,
              y$actual_wicket_pct,
              y$avg_exp_wicket_pct))
}

cat("\n")
cat("=== Drift from Starting Values ===\n")
cat(sprintf("Starting values: Runs=%.2f, Survival=%.3f, Strike=%.3f\n\n",
            START_RUNS, START_SURVIVAL, 1 - START_SURVIVAL))
cat(sprintf("%-6s %12s %12s %12s %12s\n",
            "Year", "Bat Score", "Bowl Econ", "Bat Surv", "Bowl Strk"))
cat(paste(rep("-", 56), collapse = ""), "\n")

for (i in seq_len(nrow(yearly_stats))) {
  y <- yearly_stats[i, ]
  cat(sprintf("%-6.0f %+12.3f %+12.3f %+12.4f %+12.4f\n",
              y$year,
              y$avg_batter_scoring - START_RUNS,
              y$avg_bowler_economy - START_RUNS,
              y$avg_batter_survival - START_SURVIVAL,
              y$avg_bowler_strike - (1 - START_SURVIVAL)))
}

# 4.2 Top/Bottom Players ----
cli::cli_h2("Top/Bottom Players (Min 500 balls)")

MIN_BALLS <- 500

# Get final state for each player
player_final <- DBI::dbGetQuery(conn, sprintf("
  WITH batter_final AS (
    SELECT
      batter_id as player_id,
      batter_scoring_index as scoring_index,
      batter_survival_rate as survival_rate,
      batter_balls_faced as balls,
      ROW_NUMBER() OVER (PARTITION BY batter_id ORDER BY match_date DESC, delivery_id DESC) as rn
    FROM %s
  ),
  bowler_final AS (
    SELECT
      bowler_id as player_id,
      bowler_economy_index as economy_index,
      bowler_strike_rate as strike_rate,
      bowler_balls_bowled as balls,
      ROW_NUMBER() OVER (PARTITION BY bowler_id ORDER BY match_date DESC, delivery_id DESC) as rn
    FROM %s
  )
  SELECT
    COALESCE(b.player_id, w.player_id) as player_id,
    b.scoring_index as batter_scoring,
    b.survival_rate as batter_survival,
    b.balls as batter_balls,
    w.economy_index as bowler_economy,
    w.strike_rate as bowler_strike,
    w.balls as bowler_balls
  FROM (SELECT * FROM batter_final WHERE rn = 1) b
  FULL OUTER JOIN (SELECT * FROM bowler_final WHERE rn = 1) w
    ON b.player_id = w.player_id
", skill_table, skill_table))

# Join with player names
players <- DBI::dbGetQuery(conn, "SELECT player_id, player_name FROM players")
player_final <- player_final %>%
  left_join(players, by = "player_id")

# Top 10 Batters by Scoring Index
cat("\n=== Top 10 Batters by Scoring Index ===\n")
top_batters <- player_final %>%
  filter(!is.na(batter_scoring), batter_balls >= MIN_BALLS) %>%
  arrange(desc(batter_scoring)) %>%
  head(10)

cat(sprintf("%-30s %12s %12s %10s\n", "Player", "Scoring Idx", "Survival", "Balls"))
cat(paste(rep("-", 68), collapse = ""), "\n")
for (i in seq_len(nrow(top_batters))) {
  p <- top_batters[i, ]
  cat(sprintf("%-30s %12.3f %12.4f %10s\n",
              substr(p$player_name, 1, 30),
              p$batter_scoring,
              p$batter_survival,
              format(p$batter_balls, big.mark = ",")))
}

# Bottom 10 Batters by Scoring Index
cat("\n=== Bottom 10 Batters by Scoring Index ===\n")
bottom_batters <- player_final %>%
  filter(!is.na(batter_scoring), batter_balls >= MIN_BALLS) %>%
  arrange(batter_scoring) %>%
  head(10)

cat(sprintf("%-30s %12s %12s %10s\n", "Player", "Scoring Idx", "Survival", "Balls"))
cat(paste(rep("-", 68), collapse = ""), "\n")
for (i in seq_len(nrow(bottom_batters))) {
  p <- bottom_batters[i, ]
  cat(sprintf("%-30s %12.3f %12.4f %10s\n",
              substr(p$player_name, 1, 30),
              p$batter_scoring,
              p$batter_survival,
              format(p$batter_balls, big.mark = ",")))
}

# Top 10 Bowlers by Economy (lower is better)
cat("\n=== Top 10 Bowlers by Economy Index (Lower = Better) ===\n")
top_bowlers_econ <- player_final %>%
  filter(!is.na(bowler_economy), bowler_balls >= MIN_BALLS) %>%
  arrange(bowler_economy) %>%
  head(10)

cat(sprintf("%-30s %12s %12s %10s\n", "Player", "Economy Idx", "Strike Rate", "Balls"))
cat(paste(rep("-", 68), collapse = ""), "\n")
for (i in seq_len(nrow(top_bowlers_econ))) {
  p <- top_bowlers_econ[i, ]
  cat(sprintf("%-30s %12.3f %12.4f %10s\n",
              substr(p$player_name, 1, 30),
              p$bowler_economy,
              p$bowler_strike,
              format(p$bowler_balls, big.mark = ",")))
}

# Top 10 Bowlers by Strike Rate (higher is better - takes more wickets)
cat("\n=== Top 10 Bowlers by Strike Rate (Higher = More Wickets) ===\n")
top_bowlers_strike <- player_final %>%
  filter(!is.na(bowler_strike), bowler_balls >= MIN_BALLS) %>%
  arrange(desc(bowler_strike)) %>%
  head(10)

cat(sprintf("%-30s %12s %12s %10s\n", "Player", "Strike Rate", "Economy Idx", "Balls"))
cat(paste(rep("-", 68), collapse = ""), "\n")
for (i in seq_len(nrow(top_bowlers_strike))) {
  p <- top_bowlers_strike[i, ]
  cat(sprintf("%-30s %12.4f %12.3f %10s\n",
              substr(p$player_name, 1, 30),
              p$bowler_strike,
              p$bowler_economy,
              format(p$bowler_balls, big.mark = ",")))
}

# 4.3 Distribution Analysis ----
cli::cli_h2("Distribution Analysis")

# Get distributions
dist_stats <- DBI::dbGetQuery(conn, sprintf("
  SELECT
    'Batter Scoring' as metric,
    MIN(batter_scoring_index) as min_val,
    PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY batter_scoring_index) as p5,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY batter_scoring_index) as p25,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY batter_scoring_index) as median,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY batter_scoring_index) as p75,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY batter_scoring_index) as p95,
    MAX(batter_scoring_index) as max_val
  FROM %s

  UNION ALL

  SELECT
    'Bowler Economy' as metric,
    MIN(bowler_economy_index),
    PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY bowler_economy_index),
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY bowler_economy_index),
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY bowler_economy_index),
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY bowler_economy_index),
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY bowler_economy_index),
    MAX(bowler_economy_index)
  FROM %s

  UNION ALL

  SELECT
    'Batter Survival' as metric,
    MIN(batter_survival_rate),
    PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY batter_survival_rate),
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY batter_survival_rate),
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY batter_survival_rate),
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY batter_survival_rate),
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY batter_survival_rate),
    MAX(batter_survival_rate)
  FROM %s

  UNION ALL

  SELECT
    'Bowler Strike' as metric,
    MIN(bowler_strike_rate),
    PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY bowler_strike_rate),
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY bowler_strike_rate),
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY bowler_strike_rate),
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY bowler_strike_rate),
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY bowler_strike_rate),
    MAX(bowler_strike_rate)
  FROM %s
", skill_table, skill_table, skill_table, skill_table))

cat("\n=== Skill Index Distributions ===\n")
cat(sprintf("%-16s %8s %8s %8s %8s %8s %8s %8s\n",
            "Metric", "Min", "P5", "P25", "Median", "P75", "P95", "Max"))
cat(paste(rep("-", 80), collapse = ""), "\n")

for (i in seq_len(nrow(dist_stats))) {
  d <- dist_stats[i, ]
  cat(sprintf("%-16s %8.3f %8.3f %8.3f %8.3f %8.3f %8.3f %8.3f\n",
              d$metric,
              d$min_val, d$p5, d$p25, d$median, d$p75, d$p95, d$max_val))
}

# 4.4 Prediction Accuracy ----
cli::cli_h2("Prediction Accuracy")

accuracy <- DBI::dbGetQuery(conn, sprintf("
  SELECT
    -- Runs prediction
    AVG(actual_runs) as actual_mean_runs,
    AVG(exp_runs) as predicted_mean_runs,
    AVG(ABS(actual_runs - exp_runs)) as mae_runs,
    SQRT(AVG(POWER(actual_runs - exp_runs, 2))) as rmse_runs,
    CORR(actual_runs, exp_runs) as corr_runs,

    -- Wicket prediction
    AVG(CAST(is_wicket AS DOUBLE)) as actual_wicket_rate,
    AVG(exp_wicket) as predicted_wicket_rate,

    -- By skill level buckets
    COUNT(*) as n_total
  FROM %s
", skill_table))

cat("\n=== Runs Prediction ===\n")
cat(sprintf("  Actual mean:     %.3f runs/ball\n", accuracy$actual_mean_runs))
cat(sprintf("  Predicted mean:  %.3f runs/ball\n", accuracy$predicted_mean_runs))
cat(sprintf("  MAE:             %.3f runs\n", accuracy$mae_runs))
cat(sprintf("  RMSE:            %.3f runs\n", accuracy$rmse_runs))
cat(sprintf("  Correlation:     %.3f\n", accuracy$corr_runs))

cat("\n=== Wicket Prediction ===\n")
cat(sprintf("  Actual rate:     %.2f%%\n", accuracy$actual_wicket_rate * 100))
cat(sprintf("  Predicted rate:  %.2f%%\n", accuracy$predicted_wicket_rate * 100))

# Accuracy by skill quintile
cat("\n=== Runs by Batter Skill Quintile ===\n")
quintile_stats <- DBI::dbGetQuery(conn, sprintf("
  WITH ranked AS (
    SELECT
      batter_scoring_index,
      actual_runs,
      exp_runs,
      NTILE(5) OVER (ORDER BY batter_scoring_index) as quintile
    FROM %s
  )
  SELECT
    quintile,
    AVG(batter_scoring_index) as avg_skill,
    AVG(actual_runs) as actual_runs,
    AVG(exp_runs) as exp_runs,
    COUNT(*) as n
  FROM ranked
  GROUP BY quintile
  ORDER BY quintile
", skill_table))

cat(sprintf("%-10s %12s %12s %12s %12s\n",
            "Quintile", "Avg Skill", "Actual Runs", "Exp Runs", "Count"))
cat(paste(rep("-", 60), collapse = ""), "\n")
for (i in seq_len(nrow(quintile_stats))) {
  q <- quintile_stats[i, ]
  cat(sprintf("%-10d %12.3f %12.3f %12.3f %12s\n",
              q$quintile,
              q$avg_skill,
              q$actual_runs,
              q$exp_runs,
              format(q$n, big.mark = ",")))
}

# 4.5 Player Trajectory Examples ----
cli::cli_h2("Player Trajectory Examples")

# Find some well-known players with lots of data
example_players <- DBI::dbGetQuery(conn, sprintf("
  SELECT
    batter_id as player_id,
    COUNT(*) as n_balls
  FROM %s
  GROUP BY batter_id
  HAVING COUNT(*) > 1000
  ORDER BY COUNT(*) DESC
  LIMIT 5
", skill_table))

if (nrow(example_players) > 0) {
  for (pid in example_players$player_id) {
    player_name <- players$player_name[players$player_id == pid]
    if (length(player_name) == 0) player_name <- pid

    trajectory <- DBI::dbGetQuery(conn, sprintf("
      SELECT
        EXTRACT(YEAR FROM match_date) as year,
        COUNT(*) as balls,
        AVG(batter_scoring_index) as avg_scoring,
        AVG(batter_survival_rate) as avg_survival,
        AVG(actual_runs) as actual_runs
      FROM %s
      WHERE batter_id = '%s'
      GROUP BY EXTRACT(YEAR FROM match_date)
      ORDER BY year
    ", skill_table, pid))

    if (nrow(trajectory) > 1) {
      cat(sprintf("\n=== %s ===\n", player_name))
      cat(sprintf("%-6s %8s %12s %12s %12s\n",
                  "Year", "Balls", "Scoring Idx", "Survival", "Actual Runs"))
      cat(paste(rep("-", 54), collapse = ""), "\n")

      for (i in seq_len(nrow(trajectory))) {
        t <- trajectory[i, ]
        cat(sprintf("%-6.0f %8s %12.3f %12.4f %12.3f\n",
                    t$year,
                    format(t$balls, big.mark = ","),
                    t$avg_scoring,
                    t$avg_survival,
                    t$actual_runs))
      }
    }
  }
}

# 4.6 Sanity Checks ----
cli::cli_h2("Sanity Checks")

# Check for impossible values
sanity <- DBI::dbGetQuery(conn, sprintf("
  SELECT
    SUM(CASE WHEN batter_scoring_index < 0 THEN 1 ELSE 0 END) as neg_batter_scoring,
    SUM(CASE WHEN bowler_economy_index < 0 THEN 1 ELSE 0 END) as neg_bowler_economy,
    SUM(CASE WHEN batter_survival_rate < 0 OR batter_survival_rate > 1 THEN 1 ELSE 0 END) as invalid_survival,
    SUM(CASE WHEN bowler_strike_rate < 0 OR bowler_strike_rate > 1 THEN 1 ELSE 0 END) as invalid_strike,
    SUM(CASE WHEN exp_runs < 0 THEN 1 ELSE 0 END) as neg_exp_runs,
    SUM(CASE WHEN exp_wicket < 0 OR exp_wicket > 1 THEN 1 ELSE 0 END) as invalid_exp_wicket,
    COUNT(*) as total
  FROM %s
", skill_table))

cat("\n=== Value Range Checks ===\n")
cat(sprintf("  Negative batter scoring:    %d\n", sanity$neg_batter_scoring))
cat(sprintf("  Negative bowler economy:    %d\n", sanity$neg_bowler_economy))
cat(sprintf("  Invalid survival rate:      %d\n", sanity$invalid_survival))
cat(sprintf("  Invalid strike rate:        %d\n", sanity$invalid_strike))
cat(sprintf("  Negative expected runs:     %d\n", sanity$neg_exp_runs))
cat(sprintf("  Invalid expected wicket:    %d\n", sanity$invalid_exp_wicket))
cat(sprintf("  Total records:              %s\n", format(sanity$total, big.mark = ",")))

if (sanity$neg_batter_scoring == 0 &&
    sanity$neg_bowler_economy == 0 &&
    sanity$invalid_survival == 0 &&
    sanity$invalid_strike == 0) {
  cli::cli_alert_success("All sanity checks passed!")
} else {
  cli::cli_alert_warning("Some values are out of expected range")
}

# 4.7 Summary ----
cat("\n")
cli::cli_h2("Summary")

total_stats <- DBI::dbGetQuery(conn, sprintf("
  SELECT
    COUNT(*) as n_deliveries,
    COUNT(DISTINCT match_id) as n_matches,
    COUNT(DISTINCT batter_id) as n_batters,
    COUNT(DISTINCT bowler_id) as n_bowlers,
    MIN(match_date) as first_date,
    MAX(match_date) as last_date
  FROM %s
", skill_table))

cat(sprintf("  Total deliveries:  %s\n", format(total_stats$n_deliveries, big.mark = ",")))
cat(sprintf("  Total matches:     %s\n", format(total_stats$n_matches, big.mark = ",")))
cat(sprintf("  Unique batters:    %s\n", format(total_stats$n_batters, big.mark = ",")))
cat(sprintf("  Unique bowlers:    %s\n", format(total_stats$n_bowlers, big.mark = ",")))
cat(sprintf("  Date range:        %s to %s\n", total_stats$first_date, total_stats$last_date))

cat("\n")
cli::cli_alert_success("{toupper(current_format)} validation complete!")

}  # End of format loop

# 5. Final Summary ----
cat("\n")
cli::cli_h1("Validation Complete")
cli::cli_alert_success("Validated formats: {paste(toupper(formats_to_process), collapse = ', ')}")
cat("\n")
