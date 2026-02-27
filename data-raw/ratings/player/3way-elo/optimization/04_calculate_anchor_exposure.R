# 07 Calculate Anchor Exposure for Player Calibration ----
#
# This script computes player anchor exposure and calibration scores
# for the 3-way ELO graph-based calibration system.
#
# Calibration Score Components:
#   1. Direct exposure: % balls in anchor events (IPL, BBL, CPL, PSL, Hundred, SA20, T20WC)
#   2. Indirect exposure: Weighted avg anchor% of opponents faced
#   3. Combined score: 0.7 * direct + 0.3 * indirect
#
# Output:
#   - {format}_player_calibration table in DuckDB
#   - CSV export for debugging
#
# Prerequisites:
#   - Run 04_calculate_3way_elo.R first to populate 3-way ELO tables

# 1. Setup ----
library(DBI)
library(data.table)
devtools::load_all()

# 2. Configuration ----
FORMAT <- "t20"                    # Format to calculate calibration for
DIRECT_WEIGHT <- 0.7               # Weight for direct anchor exposure
INDIRECT_WEIGHT <- 0.3             # Weight for indirect anchor exposure

# Anchor events (premier leagues with strong player pools)
ANCHOR_EVENTS <- c(
  "Indian Premier League",
  "Big Bash League",
  "Pakistan Super League",
  "Caribbean Premier League",
  "SA20",
  "The Hundred Men's Competition",
  "The Hundred Women's Competition",
  "Major League Cricket"
)

# Also include main T20 World Cup events (not qualifiers)
ANCHOR_WC_PATTERN <- "T20 World Cup"
EXCLUDE_WC_PATTERN <- "Qualifier"

cat("\n")
cli::cli_h1("Anchor Exposure Calculation")
cli::cli_alert_info("Format: {toupper(FORMAT)}")
cli::cli_alert_info("Anchor events: {length(ANCHOR_EVENTS)}")
cli::cli_alert_info("Direct weight: {DIRECT_WEIGHT}, Indirect weight: {INDIRECT_WEIGHT}")
cat("\n")

# 3. Database Connection ----
cli::cli_h2("Connecting to database")
conn <- get_db_connection(read_only = FALSE)  # Need write for saving table
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
cli::cli_alert_success("Connected to database")

# 4. Build Anchor Event SQL Filter ----
cli::cli_h2("Building anchor event filter")

# Create SQL LIKE patterns for anchor events
anchor_patterns <- sapply(ANCHOR_EVENTS, function(e) {
  sprintf("m.event_name LIKE '%%%s%%'", e)
})
anchor_sql <- paste(anchor_patterns, collapse = " OR ")

# Add T20 World Cup (excluding qualifiers)
anchor_sql <- sprintf("(%s OR (m.event_name LIKE '%%%s%%' AND m.event_name NOT LIKE '%%%s%%'))",
                      anchor_sql, ANCHOR_WC_PATTERN, EXCLUDE_WC_PATTERN)

cli::cli_alert_info("Anchor SQL filter: {substr(anchor_sql, 1, 100)}...")

# 5. Calculate Player Anchor Exposure ----
cli::cli_h2("Calculating direct anchor exposure")

elo_table <- sprintf("%s_3way_elo", FORMAT)

# Check if table exists
if (!elo_table %in% DBI::dbListTables(conn)) {
  cli::cli_alert_danger("Table {elo_table} not found! Run 04_calculate_3way_elo.R first.")
  stop("ELO table not found")
}

# Get anchor exposure for all players (as both batter and bowler)
query_direct <- sprintf("
WITH batter_balls AS (
  SELECT
    e.batter_id AS player_id,
    COUNT(*) AS total_balls,
    SUM(CASE WHEN %s THEN 1 ELSE 0 END) AS anchor_balls
  FROM %s e
  JOIN cricsheet.matches m ON e.match_id = m.match_id
  GROUP BY e.batter_id
),
bowler_balls AS (
  SELECT
    e.bowler_id AS player_id,
    COUNT(*) AS total_balls,
    SUM(CASE WHEN %s THEN 1 ELSE 0 END) AS anchor_balls
  FROM %s e
  JOIN cricsheet.matches m ON e.match_id = m.match_id
  GROUP BY e.bowler_id
),
combined AS (
  SELECT player_id, SUM(total_balls) AS total_balls, SUM(anchor_balls) AS anchor_balls
  FROM (
    SELECT * FROM batter_balls
    UNION ALL
    SELECT * FROM bowler_balls
  ) all_balls
  GROUP BY player_id
)
SELECT
  player_id,
  total_balls,
  anchor_balls,
  ROUND(anchor_balls * 100.0 / NULLIF(total_balls, 0), 2) AS direct_anchor_pct
FROM combined
WHERE total_balls > 0
", anchor_sql, elo_table, anchor_sql, elo_table)

player_direct <- DBI::dbGetQuery(conn, query_direct)
setDT(player_direct)

cli::cli_alert_success("Calculated direct exposure for {nrow(player_direct)} players")

# Summary stats
cat("\n--- Direct Anchor Exposure Distribution ---\n")
cat(sprintf("Players with >0%% anchor: %d (%.1f%%)\n",
           sum(player_direct$direct_anchor_pct > 0),
           sum(player_direct$direct_anchor_pct > 0) / nrow(player_direct) * 100))
cat(sprintf("Players with >10%% anchor: %d (%.1f%%)\n",
           sum(player_direct$direct_anchor_pct > 10),
           sum(player_direct$direct_anchor_pct > 10) / nrow(player_direct) * 100))
cat(sprintf("Players with >50%% anchor: %d (%.1f%%)\n",
           sum(player_direct$direct_anchor_pct > 50),
           sum(player_direct$direct_anchor_pct > 50) / nrow(player_direct) * 100))
cat(sprintf("Players with 100%% anchor: %d (%.1f%%)\n",
           sum(player_direct$direct_anchor_pct == 100),
           sum(player_direct$direct_anchor_pct == 100) / nrow(player_direct) * 100))

# 6. Build Player Interaction Graph ----
cli::cli_h2("Building player interaction graph")

query_interactions <- sprintf("
SELECT
  batter_id,
  bowler_id,
  COUNT(*) AS balls
FROM %s
GROUP BY batter_id, bowler_id
", elo_table)

interactions <- DBI::dbGetQuery(conn, query_interactions)
setDT(interactions)

cli::cli_alert_success("Found {format(nrow(interactions), big.mark = ',')} batter-bowler pairs")

# 7. Calculate Indirect Anchor Exposure ----
cli::cli_h2("Calculating indirect anchor exposure")

# Merge interactions with player anchor percentages
player_anchor_lookup <- player_direct[, .(player_id, direct_anchor_pct)]

# For batters: weighted average anchor% of bowlers faced
batter_opp <- merge(
  interactions,
  player_anchor_lookup,
  by.x = "bowler_id",
  by.y = "player_id",
  all.x = TRUE
)
batter_opp[is.na(direct_anchor_pct), direct_anchor_pct := 0]

batter_indirect <- batter_opp[, .(
  weighted_anchor = sum(direct_anchor_pct * balls),
  total_balls = sum(balls)
), by = batter_id]
batter_indirect[, opp_anchor_pct := weighted_anchor / total_balls]

# For bowlers: weighted average anchor% of batters faced
bowler_opp <- merge(
  interactions,
  player_anchor_lookup,
  by.x = "batter_id",
  by.y = "player_id",
  all.x = TRUE
)
bowler_opp[is.na(direct_anchor_pct), direct_anchor_pct := 0]

bowler_indirect <- bowler_opp[, .(
  weighted_anchor = sum(direct_anchor_pct * balls),
  total_balls = sum(balls)
), by = bowler_id]
bowler_indirect[, opp_anchor_pct := weighted_anchor / total_balls]

# Combine (take max for players who are both batters and bowlers)
player_indirect <- rbind(
  batter_indirect[, .(player_id = batter_id, opp_anchor_pct)],
  bowler_indirect[, .(player_id = bowler_id, opp_anchor_pct)]
)
player_indirect <- player_indirect[, .(opp_anchor_pct = max(opp_anchor_pct)), by = player_id]

cli::cli_alert_success("Calculated indirect exposure for {nrow(player_indirect)} players")

# Summary stats
cat("\n--- Indirect Anchor Exposure Distribution ---\n")
cat(sprintf("Mean opponent anchor%%: %.1f%%\n", mean(player_indirect$opp_anchor_pct)))
cat(sprintf("Median opponent anchor%%: %.1f%%\n", median(player_indirect$opp_anchor_pct)))
cat(sprintf("Players with >10%% opponent anchor: %d (%.1f%%)\n",
           sum(player_indirect$opp_anchor_pct > 10),
           sum(player_indirect$opp_anchor_pct > 10) / nrow(player_indirect) * 100))

# 8. Calculate Combined Calibration Score ----
cli::cli_h2("Calculating combined calibration score")

# Merge direct and indirect exposure
calibration <- merge(
  player_direct[, .(player_id, total_balls, anchor_balls, direct_anchor_pct)],
  player_indirect,
  by = "player_id",
  all = TRUE
)

# Fill NAs
calibration[is.na(direct_anchor_pct), direct_anchor_pct := 0]
calibration[is.na(opp_anchor_pct), opp_anchor_pct := 0]

# Combined calibration score (capped at 100)
calibration[, calibration_score := pmin(100,
  direct_anchor_pct * DIRECT_WEIGHT +
  opp_anchor_pct * INDIRECT_WEIGHT
)]

# Get player names for debugging
player_names <- DBI::dbGetQuery(conn, "SELECT player_id, player_name FROM cricsheet.players")
setDT(player_names)
calibration <- merge(calibration, player_names, by = "player_id", all.x = TRUE)

cli::cli_alert_success("Calculated calibration for {nrow(calibration)} players")

# Summary stats
cat("\n--- Calibration Score Distribution ---\n")
cat(sprintf("Mean: %.1f\n", mean(calibration$calibration_score)))
cat(sprintf("Median: %.1f\n", median(calibration$calibration_score)))
cat(sprintf("Players with score 0-5: %d\n", sum(calibration$calibration_score <= 5)))
cat(sprintf("Players with score 5-20: %d\n",
           sum(calibration$calibration_score > 5 & calibration$calibration_score <= 20)))
cat(sprintf("Players with score 20-50: %d\n",
           sum(calibration$calibration_score > 20 & calibration$calibration_score <= 50)))
cat(sprintf("Players with score 50+: %d\n", sum(calibration$calibration_score > 50)))

# 9. Get Current ELOs for Context ----
cli::cli_h2("Getting current ELOs")

query_elos <- sprintf("
WITH latest AS (
  SELECT
    batter_id AS player_id,
    batter_run_elo_after AS run_elo,
    batter_wicket_elo_after AS wicket_elo,
    ROW_NUMBER() OVER (PARTITION BY batter_id ORDER BY match_date DESC, delivery_id DESC) AS rn
  FROM %s
)
SELECT player_id, run_elo, wicket_elo
FROM latest
WHERE rn = 1
", elo_table)

player_elos <- DBI::dbGetQuery(conn, query_elos)
setDT(player_elos)

calibration <- merge(calibration, player_elos, by = "player_id", all.x = TRUE)

# 10. Show Examples ----
cli::cli_h2("Example players")

cat("\n--- Top 20 by Calibration Score ---\n")
top_calibrated <- calibration[order(-calibration_score)][1:20]
print(top_calibrated[, .(player_name, total_balls, direct_anchor_pct, opp_anchor_pct,
                         calibration_score, run_elo)])

cat("\n--- Bottom 20 by Calibration Score (with >500 balls) ---\n")
bottom_calibrated <- calibration[total_balls > 500][order(calibration_score)][1:20]
print(bottom_calibrated[, .(player_name, total_balls, direct_anchor_pct, opp_anchor_pct,
                            calibration_score, run_elo)])

cat("\n--- High ELO + Low Calibration (potential inflation) ---\n")
inflation_suspects <- calibration[run_elo > 1600 & calibration_score < 20][order(-run_elo)]
if (nrow(inflation_suspects) > 0) {
  print(inflation_suspects[1:min(20, nrow(inflation_suspects)),
                           .(player_name, total_balls, direct_anchor_pct, opp_anchor_pct,
                             calibration_score, run_elo)])
} else {
  cat("No players found with high ELO and low calibration\n")
}

# 11. Save to Database ----
cli::cli_h2("Saving to database")

output_table <- sprintf("%s_player_calibration", FORMAT)

# Prepare table for saving
calibration_save <- calibration[, .(
  player_id,
  player_name,
  total_balls,
  anchor_balls,
  direct_anchor_pct,
  opp_anchor_pct,
  calibration_score,
  run_elo,
  wicket_elo
)]

# Drop existing table if exists
if (output_table %in% DBI::dbListTables(conn)) {
  DBI::dbExecute(conn, sprintf("DROP TABLE %s", output_table))
  cli::cli_alert_info("Dropped existing {output_table} table")
}

# Write new table
DBI::dbWriteTable(conn, output_table, calibration_save, overwrite = TRUE)
cli::cli_alert_success("Saved {nrow(calibration_save)} rows to {output_table}")

# 12. Save CSV for Debugging ----
csv_path <- file.path(find_bouncerdata_dir(), "temp_3way_elo",
                      sprintf("%s_player_calibration.csv", FORMAT))
dir.create(dirname(csv_path), showWarnings = FALSE, recursive = TRUE)
fwrite(calibration_save, csv_path)
cli::cli_alert_success("Saved CSV to {csv_path}")

# 13. Summary ----
cli::cli_h1("Summary")

cat("\n")
cat(sprintf("Total players: %d\n", nrow(calibration)))
cat(sprintf("Players with anchor exposure: %d (%.1f%%)\n",
           sum(calibration$direct_anchor_pct > 0),
           sum(calibration$direct_anchor_pct > 0) / nrow(calibration) * 100))
cat(sprintf("Mean calibration score: %.1f\n", mean(calibration$calibration_score)))
cat(sprintf("Players with calibration < 5: %d\n", sum(calibration$calibration_score < 5)))
cat(sprintf("High ELO + low calibration suspects: %d\n",
           nrow(calibration[run_elo > 1600 & calibration_score < 20])))
cat("\n")

cli::cli_alert_success("Anchor exposure calculation complete!")
cat("\n")
cli::cli_h3("Next Steps")
cli::cli_bullets(c(
  "i" = "Use calibration scores in get_3way_player_k() for K-factor adjustment",
  "i" = "Apply sample-size blending in predictions",
  "i" = "Re-run 06_validate_3way_elo.R to measure improvement"
))
cat("\n")
