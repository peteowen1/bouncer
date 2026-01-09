# Debug script: Understanding Margin of Victory (MOV) Calculation
#
# This script explains how unified_margin and MOV multiplier work
# Run from bouncer/ directory

library(cli)
library(dplyr)
devtools::load_all()

cli::cli_h1("Understanding Margin of Victory (MOV) in Cricket ELO")

# Part 1: Resource Calculation ----

cli::cli_h1("Part 1: Resource Calculation")

cat("
The 'resource' model is based on DLS (Duckworth-Lewis-Stern) principles.
Each combination of (overs remaining, wickets remaining) represents a
percentage of total scoring potential.

Our Formula (DLS-optimized via grid search):
  overs_factor = (overs_remaining / max_overs) ^ overs_power
  wickets_factor = (wickets_remaining / 10) ^ wickets_power
  resource_pct = overs_factor * wickets_factor

Optimized Parameters (minimizing RMSE from DLS tables):
  - T20:  overs_power=0.74, wickets_power=0.82 (RMSE: 3.76%)
  - ODI:  overs_power=0.59, wickets_power=1.10 (RMSE: 4.19%)
  - Test: overs_power=0.40, wickets_power=0.85 (no DLS benchmark)

Critical edge cases:
  - 0 wickets = 0% (all out - can't score)
  - 0 overs = 0% (innings over in limited overs)
")

cli::cli_h2("Comparison: Our Formula vs Actual DLS")

cat("
ACTUAL DLS TABLE VALUES (from ICC Standard Edition):
====================================================
DLS uses a lookup table, NOT a simple formula. Here are known values:

T20 (20 overs max):
  Overs  | 10 wkts | 5 wkts | 1 wkt
  -------|---------|--------|------
  20     | 100.0%  |  ~50%  |  8.3%
  10     |  56.7%  |  ~32%  |  ~5%
   5     |  ~32%   |  ~20%  |  ~3%
   0     |   0.0%  |   0.0% |  0.0%

ODI (50 overs max):
  Overs  | 10 wkts | 5 wkts | 1 wkt
  -------|---------|--------|------
  50     | 100.0%  |  ~40%  |  4.7%
  30     |  75.1%  |  ~32%  |  ~4%
  10     |  32.1%  |  17.2% |  ~2%
   0     |   0.0%  |   0.0% |  0.0%

KEY DLS PROPERTIES:
1. 0 wickets remaining = 0% (ALL OUT)
2. 0 overs remaining = 0% (innings over)
3. First wickets cost MORE than last wickets
4. Relationship is NOT simply multiplicative
5. At max overs & 10 wickets = exactly 100%
")

cli::cli_h2("Testing Our Current Formula vs DLS")

cat("\n--- T20 Comparison ---\n")
cat(sprintf("%-12s %-12s %-12s %-12s\n", "Scenario", "DLS Value", "Our Value", "Error"))
cat(paste(rep("-", 52), collapse = ""), "\n")

# Known DLS T20 values (overs, wickets_remaining, dls_pct)
dls_t20 <- list(
  c(20, 10, 100.0),
  c(10, 10, 56.7),
  c(5, 10, 32.0),
  c(20, 5, 50.0),
  c(10, 5, 32.0),
  c(5, 5, 20.0),
  c(20, 1, 8.3),
  c(10, 1, 5.0),
  c(0, 10, 0.0),
  c(10, 0, 0.0)
)

for (vals in dls_t20) {
  overs <- vals[1]
  wkts <- vals[2]
  dls <- vals[3]
  ours <- calculate_resource_remaining(overs, wkts, "t20") * 100
  err <- ours - dls
  cat(sprintf("%-12s %11.1f%% %11.1f%% %+11.1f%%\n",
              sprintf("%do/%dw", overs, wkts), dls, ours, err))
}

cat("\n--- ODI Comparison ---\n")
cat(sprintf("%-12s %-12s %-12s %-12s\n", "Scenario", "DLS Value", "Our Value", "Error"))
cat(paste(rep("-", 52), collapse = ""), "\n")

dls_odi <- list(
  c(50, 10, 100.0),
  c(30, 10, 75.1),
  c(10, 10, 32.1),
  c(50, 5, 40.0),
  c(30, 5, 32.0),
  c(10, 5, 17.2),
  c(50, 1, 4.7),
  c(0, 10, 0.0),
  c(30, 0, 0.0)
)

for (vals in dls_odi) {
  overs <- vals[1]
  wkts <- vals[2]
  dls <- vals[3]
  ours <- calculate_resource_remaining(overs, wkts, "odi") * 100
  err <- ours - dls
  cat(sprintf("%-12s %11.1f%% %11.1f%% %+11.1f%%\n",
              sprintf("%do/%dw", overs, wkts), dls, ours, err))
}

cli::cli_h2("Resource Remaining Examples (Current Formula)")

cat("\n--- T20 Examples ---\n")
scenarios_t20 <- expand.grid(
  overs_remaining = c(0, 2, 5, 10),
  wickets_remaining = c(0, 3, 6, 10)
)

for (i in seq_len(nrow(scenarios_t20))) {
  overs <- scenarios_t20$overs_remaining[i]
  wickets <- scenarios_t20$wickets_remaining[i]
  resource <- calculate_resource_remaining(overs, wickets, "t20")
  cat(sprintf("  %2d overs, %2d wickets left -> %.1f%% resources\n",
              overs, wickets, resource * 100))
}

cat("\n--- Test Examples ---\n")
scenarios_test <- expand.grid(
  overs_remaining = c(0, 30, 60, 90),
  wickets_remaining = c(0, 3, 6, 10)
)

for (i in seq_len(nrow(scenarios_test))) {
  overs <- scenarios_test$overs_remaining[i]
  wickets <- scenarios_test$wickets_remaining[i]
  resource <- calculate_resource_remaining(overs, wickets, "test")
  cat(sprintf("  %2d overs, %2d wickets left -> %.1f%% resources\n",
              overs, wickets, resource * 100))
}

# Part 2: Unified Margin Calculation ----

cli::cli_h1("Part 2: Unified Margin Calculation")

cat("
The unified_margin converts all win types to a runs-equivalent scale:

1. RUNS WIN (batting first team wins):
   margin = team1_score - team2_score
   Simple difference - positive means team1 won.

2. WICKETS WIN (chasing team wins):
   - Project what chaser 'would have scored' using remaining resources
   - projected_score = actual_score / resource_used
   - margin = team1_score - projected_score (NEGATIVE = team2 won)

3. DRAW/TIE:
   margin = 0
")

cli::cli_h2("Runs Win Examples")

cat("\n--- Team batting first wins ---\n")
runs_examples <- list(
  list(t1 = 350, t2 = 280, desc = "Team1 wins by 70 runs"),
  list(t1 = 400, t2 = 150, desc = "Team1 wins by 250 runs (blowout)"),
  list(t1 = 200, t2 = 195, desc = "Team1 wins by 5 runs (close)")
)

for (ex in runs_examples) {
  margin <- calculate_unified_margin(ex$t1, ex$t2, 0, 0, "runs", "test")
  cat(sprintf("  %s: %d vs %d -> margin = %+d\n", ex$desc, ex$t1, ex$t2, margin))
}

cli::cli_h2("Wickets Win Examples - How wickets affect margin")

cat("\n--- Team chasing wins with different wickets remaining ---\n")
cat("Scenario: Team1 scored 300, Team2 chases and wins\n\n")

team1_score <- 300

for (wickets in c(1, 3, 5, 6, 7, 8, 9, 10)) {
  # Assume they scored 305 to win (just over target)
  team2_score <- 305
  overs_remaining <- 20  # 20 overs left

  margin <- calculate_unified_margin(
    team1_score, team2_score,
    overs_remaining, wickets,
    "wickets", "test"
  )

  # Also show the projected score
  projected <- project_chaser_final_score(team2_score, overs_remaining, wickets, "test")

  cat(sprintf("  Won by %2d wickets (20 overs left): projected=%4.0f, margin=%+4.0f\n",
              wickets, projected, margin))
}

cat("\n--- Same scenario with fewer overs remaining ---\n")
cat("Scenario: Team1 scored 300, Team2 chases with only 5 overs left\n\n")

for (wickets in c(1, 3, 5, 7, 10)) {
  team2_score <- 305
  overs_remaining <- 5

  margin <- calculate_unified_margin(
    team1_score, team2_score,
    overs_remaining, wickets,
    "wickets", "test"
  )

  projected <- project_chaser_final_score(team2_score, overs_remaining, wickets, "test")

  cat(sprintf("  Won by %2d wickets (5 overs left): projected=%4.0f, margin=%+4.0f\n",
              wickets, projected, margin))
}

# Part 3: Draws and Ties ----

cli::cli_h1("Part 3: Draws and Ties")

cat("
In Test cricket, DRAWS are common. How are they handled?

- DRAW: margin = 0 (no winner, no ELO change for result)
- TIE: margin = 0 (scores level, extremely rare in Tests)
- NO RESULT: margin = 0

For ELO purposes, draws don't update ratings based on result.
")

cli::cli_h2("Draw/Tie Examples")

draw_margin <- calculate_unified_margin(400, 300, 0, 0, "draw", "test")
cat(sprintf("  Draw (any scores): margin = %d\n", draw_margin))

tie_margin <- calculate_unified_margin(300, 300, 0, 0, "tie", "test")
cat(sprintf("  Tie (scores equal): margin = %d\n", tie_margin))

# Part 4: Follow-On and Innings Victories ----

cli::cli_h1("Part 4: Follow-On and Innings Victories")

cat("
FOLLOW-ON: When team batting first leads by 200+ runs, they can enforce follow-on.
This often leads to 'innings and X runs' victories.

How is 'innings and X runs' represented?
- Team1 bats once: 500
- Team2 bats twice: 150 + 200 = 350
- Team1 wins by 'innings and 150 runs'

In our system, this is stored as a RUNS win:
  margin = team1_innings - (team2_innings1 + team2_innings2)
  margin = 500 - 350 = 150

But wait - the database stores unified_margin directly. Let's check what
actual follow-on matches look like in the database...
")

# Connect and find follow-on examples
conn <- get_db_connection(read_only = TRUE)

cli::cli_h2("Real Follow-On Examples from Database")

# Look for large margins in Test matches (likely innings victories)
query <- "
  SELECT
    match_id,
    match_date,
    team1,
    team2,
    outcome_winner,
    outcome_type,
    outcome_by_runs,
    outcome_by_wickets,
    unified_margin
  FROM matches
  WHERE match_type IN ('Test', 'MDM')
    AND gender = 'male'
    AND team_type = 'international'
    AND unified_margin IS NOT NULL
  ORDER BY ABS(unified_margin) DESC
  LIMIT 20
"

big_margins <- DBI::dbGetQuery(conn, query)

cat("\nTop 20 Test matches by absolute margin:\n\n")
cat(sprintf("%-12s %-20s %-20s %8s %8s %8s\n",
            "Date", "Winner", "Loser", "ByRuns", "ByWkts", "Unified"))
cat(paste(rep("-", 85), collapse = ""), "\n")

for (i in seq_len(nrow(big_margins))) {
  row <- big_margins[i, ]
  winner <- row$outcome_winner
  loser <- if (winner == row$team1) row$team2 else row$team1

  by_runs <- if (!is.na(row$outcome_by_runs)) sprintf("%d", row$outcome_by_runs) else "-"
  by_wkts <- if (!is.na(row$outcome_by_wickets)) sprintf("%d", row$outcome_by_wickets) else "-"

  cat(sprintf("%-12s %-20s %-20s %8s %8s %+8.0f\n",
              as.character(row$match_date),
              substr(winner, 1, 20),
              substr(loser, 1, 20),
              by_runs, by_wkts,
              row$unified_margin))
}

# Check for innings victories - these typically have very large margins and no wickets
cli::cli_h2("Innings Victory Analysis")

cat("\nInnings victories are identified by large margins where outcome_by_wickets is NULL\n")
cat("(Innings wins are stored as runs wins in the database)\n\n")

# Large margins with runs wins (likely innings victories)
innings_query <- "
  SELECT
    match_id,
    match_date,
    team1,
    team2,
    outcome_winner,
    outcome_by_runs,
    unified_margin
  FROM matches
  WHERE match_type IN ('Test', 'MDM')
    AND gender = 'male'
    AND team_type = 'international'
    AND outcome_by_runs IS NOT NULL
    AND outcome_by_runs > 200
  ORDER BY outcome_by_runs DESC
  LIMIT 10
"

innings_victories <- DBI::dbGetQuery(conn, innings_query)

if (nrow(innings_victories) > 0) {
  cat("Largest runs victories (likely include innings wins):\n\n")
  cat(sprintf("%-12s %-18s %-18s %8s %10s\n",
              "Date", "Winner", "Loser", "ByRuns", "Unified"))
  cat(paste(rep("-", 70), collapse = ""), "\n")
  for (i in seq_len(nrow(innings_victories))) {
    row <- innings_victories[i, ]
    loser <- if (row$outcome_winner == row$team1) row$team2 else row$team1
    cat(sprintf("%-12s %-18s %-18s %8d %+10.0f\n",
                as.character(row$match_date),
                substr(row$outcome_winner, 1, 18),
                substr(loser, 1, 18),
                row$outcome_by_runs,
                row$unified_margin))
  }
}

# Part 5: MOV Multiplier (G-Factor) ----

cli::cli_h1("Part 5: MOV Multiplier (G-Factor) in ELO Updates")

cat("
The MOV multiplier (G-factor) scales ELO updates based on margin.
It's used in the standard ELO formula:

  new_elo = old_elo + K * G * (actual - expected)

Where G (MOV multiplier) is calculated as:

  G = (|margin| + base_offset)^exponent / (denom_base + elo_factor * elo_diff)

Key properties:
1. Bigger margin -> bigger G -> more ELO change
2. Upset (underdog wins) -> smaller denominator -> bigger G
3. Expected win (favorite wins) -> bigger denominator -> smaller G
4. G is clamped between MOV_MIN and MOV_MAX
")

cli::cli_h2("MOV Multiplier Examples")

cat("\nDefault parameters: exponent=0.7, base_offset=5, denom_base=10, elo_factor=0.004\n\n")

# Show how G changes with margin
cat("--- G vs Margin (even teams, elo_diff=0) ---\n")
for (margin in c(10, 25, 50, 100, 150, 200, 300)) {
  G <- calculate_mov_multiplier(margin, 1500, 1500, "test")
  cat(sprintf("  Margin=%3d runs: G=%.3f\n", margin, G))
}

cat("\n--- G when favorite wins vs underdog wins ---\n")
cat("Margin = 50 runs\n")

# Favorite wins (1600 beats 1400)
G_fav <- calculate_mov_multiplier(50, 1600, 1400, "test")
cat(sprintf("  Favorite wins (1600 beats 1400): G=%.3f (dampened)\n", G_fav))

# Even match
G_even <- calculate_mov_multiplier(50, 1500, 1500, "test")
cat(sprintf("  Even match (1500 beats 1500):    G=%.3f (neutral)\n", G_even))

# Underdog wins (1400 beats 1600)
G_und <- calculate_mov_multiplier(50, 1400, 1600, "test")
cat(sprintf("  Underdog wins (1400 beats 1600): G=%.3f (boosted)\n", G_und))

cat("\n--- Large upset with big margin ---\n")
G_big_upset <- calculate_mov_multiplier(200, 1300, 1700, "test")
cat(sprintf("  1300 beats 1700 by 200 runs: G=%.3f (maximum impact!)\n", G_big_upset))

# Part 6: Real Match Examples ----

cli::cli_h1("Part 6: Real Match Examples")

# Get some interesting real matches
cli::cli_h2("Close Finishes (small margins)")

close_query <- "
  SELECT
    match_id,
    match_date,
    team1,
    team2,
    outcome_winner,
    outcome_by_runs,
    outcome_by_wickets,
    unified_margin
  FROM matches
  WHERE match_type IN ('Test', 'MDM')
    AND gender = 'male'
    AND team_type = 'international'
    AND unified_margin IS NOT NULL
    AND ABS(unified_margin) < 30
    AND ABS(unified_margin) > 0
  ORDER BY match_date DESC
  LIMIT 10
"

close_matches <- DBI::dbGetQuery(conn, close_query)

if (nrow(close_matches) > 0) {
  cat("\nRecent close Test finishes:\n\n")
  for (i in seq_len(nrow(close_matches))) {
    row <- close_matches[i, ]
    by_what <- if (!is.na(row$outcome_by_runs) && row$outcome_by_runs > 0) {
      sprintf("by %d runs", row$outcome_by_runs)
    } else if (!is.na(row$outcome_by_wickets) && row$outcome_by_wickets > 0) {
      sprintf("by %d wickets", row$outcome_by_wickets)
    } else {
      "result"
    }
    cat(sprintf("  %s: %s beat %s %s (unified=%+.0f)\n",
                row$match_date, row$outcome_winner,
                if (row$outcome_winner == row$team1) row$team2 else row$team1,
                by_what, row$unified_margin))
  }
}

cli::cli_h2("Wickets Wins Distribution")

wickets_query <- "
  SELECT
    outcome_by_wickets,
    COUNT(*) as n_matches,
    AVG(unified_margin) as avg_margin,
    MIN(unified_margin) as min_margin,
    MAX(unified_margin) as max_margin
  FROM matches
  WHERE match_type IN ('Test', 'MDM')
    AND gender = 'male'
    AND team_type = 'international'
    AND outcome_by_wickets IS NOT NULL
    AND outcome_by_wickets > 0
  GROUP BY outcome_by_wickets
  ORDER BY outcome_by_wickets
"

wickets_dist <- DBI::dbGetQuery(conn, wickets_query)

cat("\nUnified margin by wickets remaining:\n\n")
cat(sprintf("%-8s %8s %12s %12s %12s\n", "Wickets", "Matches", "Avg Margin", "Min", "Max"))
cat(paste(rep("-", 55), collapse = ""), "\n")

for (i in seq_len(nrow(wickets_dist))) {
  row <- wickets_dist[i, ]
  cat(sprintf("%-8d %8d %+11.0f %+11.0f %+11.0f\n",
              row$outcome_by_wickets, row$n_matches,
              row$avg_margin, row$min_margin, row$max_margin))
}

# Part 7: Format Comparison ----

cli::cli_h1("Part 7: Format Comparison")

cat("
How does the same wickets win translate across formats?

Scenario: Team scores 305 to chase 300, with 6 wickets and 10% of overs remaining
")

cli::cli_h2("Same Win Across Formats")

for (fmt in c("t20", "odi", "test")) {
  # 10% overs remaining
  max_overs <- switch(fmt, "t20" = 20, "odi" = 50, "test" = 90)
  overs_remaining <- max_overs * 0.1

  margin <- calculate_unified_margin(300, 305, overs_remaining, 6, "wickets", fmt)
  projected <- project_chaser_final_score(305, overs_remaining, 6, fmt)
  resource <- calculate_resource_remaining(overs_remaining, 6, fmt)

  cat(sprintf("\n%s (%.0f overs remaining):\n", toupper(fmt), overs_remaining))
  cat(sprintf("  Resource remaining: %.1f%%\n", resource * 100))
  cat(sprintf("  Projected score: %.0f\n", projected))
  cat(sprintf("  Unified margin: %+.0f\n", margin))
}

# Part 8: Follow-On and How It Affects MOV ----

cli::cli_h1("Part 8: Follow-On and How It Affects MOV")

cat("
THE FOLLOW-ON RULE (Test Cricket Only):
---------------------------------------
When a team batting FIRST leads by 200+ runs after both teams have
batted once, they can 'enforce the follow-on' - making the opponent
bat again IMMEDIATELY instead of batting again themselves.

Normal Test Match:
  Team1 bats (innings 1) -> Team2 bats (innings 1) ->
  Team1 bats (innings 2) -> Team2 chases (innings 2)

With Follow-On Enforced:
  Team1 bats (innings 1) -> Team2 bats (innings 1, short) ->
  Team2 bats again (innings 2) -> [Team1 either wins or bats]

IMPACT ON MOV:
--------------
Follow-on victories typically result in 'innings and X runs' wins.

Example:
  - Team1 (batting first): 550 runs
  - Team2 (1st innings): 200 runs (350 run deficit - follow-on enforced)
  - Team2 (2nd innings): 280 runs (all out)
  - Result: Team1 wins by 'innings and 70 runs'
  - How calculated: 550 - (200 + 280) = 70 runs

In our database:
  - This is stored as outcome_by_runs = 70 (or the innings margin)
  - unified_margin = +70 (positive = team batting first won)
  - It's treated just like any other runs win!

KEY INSIGHT: Follow-on victories don't need special handling in our MOV
system. The database already stores them as runs margins. The only
difference is these tend to be LARGE positive margins because the
team batting first was so dominant they didn't need to bat again.
")

cli::cli_h2("Finding Likely Follow-On Victories in Database")

# Look for innings victories - these are typically follow-on enforced
# Identified by large margins where outcome mentions 'innings'
# Note: We can't directly detect follow-on from our schema, but innings victories
# are strongly correlated with follow-on situations

innings_query2 <- "
  SELECT
    match_id,
    match_date,
    team1,
    team2,
    outcome_winner,
    outcome_type,
    outcome_by_runs,
    unified_margin
  FROM matches
  WHERE match_type IN ('Test', 'MDM')
    AND gender = 'male'
    AND team_type = 'international'
    AND outcome_type LIKE '%innings%'
  ORDER BY match_date DESC
  LIMIT 15
"

innings_results <- tryCatch(
  DBI::dbGetQuery(conn, innings_query2),
  error = function(e) {
    # If outcome_type doesn't have 'innings', try alternate approach
    data.frame()
  }
)

if (nrow(innings_results) == 0) {
  # Try to find them by large margins (>150 runs) which are likely innings victories
  cat("\n(Note: outcome_type doesn't contain 'innings' text, looking for large margins)\n")

  innings_query3 <- "
    SELECT
      match_id,
      match_date,
      team1,
      team2,
      outcome_winner,
      outcome_by_runs,
      unified_margin
    FROM matches
    WHERE match_type IN ('Test', 'MDM')
      AND gender = 'male'
      AND team_type = 'international'
      AND outcome_by_runs IS NOT NULL
      AND outcome_by_runs > 150
    ORDER BY outcome_by_runs DESC
    LIMIT 15
  "
  innings_results <- DBI::dbGetQuery(conn, innings_query3)
}

if (nrow(innings_results) > 0) {
  cat("\nLikely innings victories (large run margins):\n\n")
  cat(sprintf("%-12s %-18s %-18s %10s %12s\n",
              "Date", "Winner", "Loser", "By Runs", "Unified"))
  cat(paste(rep("-", 75), collapse = ""), "\n")

  for (i in seq_len(min(15, nrow(innings_results)))) {
    row <- innings_results[i, ]
    loser <- if (row$outcome_winner == row$team1) row$team2 else row$team1
    cat(sprintf("%-12s %-18s %-18s %10d %+12.0f\n",
                as.character(row$match_date),
                substr(row$outcome_winner, 1, 18),
                substr(loser, 1, 18),
                row$outcome_by_runs,
                row$unified_margin))
  }
}

cat("
HOW FOLLOW-ON AFFECTS ELO:
--------------------------
1. Innings victories typically have margins of 100-300+ runs
2. These get larger MOV multipliers (G-factor)
3. But G is capped at MOV_MAX (~2.5), so a 300-run win
   doesn't give 3x the ELO change of a 100-run win

Example G-factors for innings victories:
")

for (margin in c(100, 150, 200, 250, 300)) {
  G <- calculate_mov_multiplier(margin, 1500, 1500, "test")
  cat(sprintf("  Innings victory by %d runs: G=%.3f\n", margin, G))
}

# Part 9: Data Validation and Parameter Optimization ----

cli::cli_h1("Part 9: Data Validation of Resource Wicket Values")

cat("
WHERE DO THE WICKET VALUES COME FROM?
=====================================
The constants RESOURCE_WICKET_VALUE_T20=7, ODI=9, TEST=12 are
ESTIMATES based on DLS resource tables and cricket intuition.

The idea: 'Each remaining wicket is worth X balls of potential scoring'

But are these values correct? Let's TEST them against real data!

VALIDATION APPROACH:
1. For WICKETS wins, we project what the chaser 'would have scored'
2. If our wicket values are correct, projected scores should align
   with actual scoring patterns in similar situations
3. We can estimate the 'true' wicket value from data and compare
")

cli::cli_h2("Test 1: Wickets Win Margin Distribution by Format")

# Get wickets wins for each format
format_wickets_query <- "
  SELECT
    CASE
      WHEN match_type IN ('T20', 'IT20') THEN 't20'
      WHEN match_type IN ('ODI', 'ODM') THEN 'odi'
      WHEN match_type IN ('Test', 'MDM') THEN 'test'
    END as format,
    gender,
    outcome_by_wickets,
    unified_margin,
    overs_per_innings
  FROM matches
  WHERE outcome_by_wickets IS NOT NULL
    AND outcome_by_wickets > 0
    AND unified_margin IS NOT NULL
    AND match_type IN ('T20', 'IT20', 'ODI', 'ODM', 'Test', 'MDM')
"

wickets_data <- DBI::dbGetQuery(conn, format_wickets_query)

cat("\nWickets wins by format:\n")
cat(sprintf("  T20: %d matches\n", sum(wickets_data$format == "t20", na.rm = TRUE)))
cat(sprintf("  ODI: %d matches\n", sum(wickets_data$format == "odi", na.rm = TRUE)))
cat(sprintf("  Test: %d matches\n", sum(wickets_data$format == "test", na.rm = TRUE)))

cli::cli_h2("Test 2: Average Margin by Wickets Remaining (per format)")

cat("\nIf wicket values are calibrated correctly, margin should scale linearly with wickets.\n\n")

for (fmt in c("t20", "odi", "test")) {
  fmt_data <- wickets_data[wickets_data$format == fmt & !is.na(wickets_data$outcome_by_wickets), ]
  if (nrow(fmt_data) < 10) next

  wicket_value <- switch(fmt,
    "t20" = RESOURCE_WICKET_VALUE_T20,
    "odi" = RESOURCE_WICKET_VALUE_ODI,
    "test" = RESOURCE_WICKET_VALUE_TEST
  )

  cat(sprintf("--- %s (current wicket_value = %d) ---\n", toupper(fmt), wicket_value))

  # Group by wickets remaining
  agg <- fmt_data %>%
    group_by(outcome_by_wickets) %>%
    summarise(
      n = n(),
      avg_margin = mean(abs(unified_margin), na.rm = TRUE),
      sd_margin = sd(abs(unified_margin), na.rm = TRUE),
      .groups = "drop"
    ) %>%
    filter(n >= 5) %>%
    arrange(outcome_by_wickets)

  if (nrow(agg) > 0) {
    cat(sprintf("%-8s %8s %12s %12s\n", "Wickets", "N", "Avg|Margin|", "SD"))
    cat(paste(rep("-", 45), collapse = ""), "\n")
    for (j in seq_len(nrow(agg))) {
      cat(sprintf("%-8d %8d %12.1f %12.1f\n",
                  agg$outcome_by_wickets[j], agg$n[j],
                  agg$avg_margin[j], agg$sd_margin[j]))
    }

    # Calculate implied wicket value from data
    # If margin ~ a + b*wickets, then b is the 'runs per additional wicket'
    if (nrow(agg) >= 3) {
      fit <- lm(avg_margin ~ outcome_by_wickets, data = agg, weights = n)
      implied_value <- coef(fit)["outcome_by_wickets"]
      cat(sprintf("\nImplied runs per wicket from data: %.1f\n", implied_value))
      cat(sprintf("Current setting: %d balls = ~%.0f runs per wicket (at 1.0-1.5 runs/ball)\n\n",
                  wicket_value, wicket_value * 1.25))
    }
  }
}

cli::cli_h2("Test 3: Projected vs Actual Scoring Rate Validation")

cat("
For wickets wins, we project: final_score = actual_score / resource_used

The 'implied run rate' tells us if projections are reasonable.
If projected scores are wildly high/low, our wicket values are wrong.
")

# Get more detailed data for analysis
detailed_query <- "
  SELECT
    m.match_id,
    CASE
      WHEN m.match_type IN ('T20', 'IT20') THEN 't20'
      WHEN m.match_type IN ('ODI', 'ODM') THEN 'odi'
      WHEN m.match_type IN ('Test', 'MDM') THEN 'test'
    END as format,
    m.gender,
    m.outcome_by_wickets,
    m.unified_margin,
    m.overs_per_innings,
    inn2.total_runs as chaser_score,
    inn2.total_overs as chaser_overs,
    inn1.total_runs as target_minus_1
  FROM matches m
  LEFT JOIN match_innings inn1 ON m.match_id = inn1.match_id AND inn1.innings = 1
  LEFT JOIN match_innings inn2 ON m.match_id = inn2.match_id AND inn2.innings = 2
  WHERE m.outcome_by_wickets IS NOT NULL
    AND m.outcome_by_wickets > 0
    AND inn2.total_runs IS NOT NULL
    AND inn2.total_overs IS NOT NULL
    AND m.overs_per_innings IS NOT NULL
    AND m.match_type IN ('T20', 'IT20', 'ODI', 'ODM')
"

detailed_data <- DBI::dbGetQuery(conn, detailed_query)

if (nrow(detailed_data) > 0) {
  cat("\n--- Analyzing projected vs actual scoring ---\n\n")

  for (fmt in c("t20", "odi")) {
    fmt_data <- detailed_data[detailed_data$format == fmt, ]
    if (nrow(fmt_data) < 20) next

    wicket_value <- switch(fmt,
      "t20" = RESOURCE_WICKET_VALUE_T20,
      "odi" = RESOURCE_WICKET_VALUE_ODI
    )

    # Calculate resource used and projected final score
    fmt_data <- fmt_data %>%
      mutate(
        overs_remaining = pmax(0.01, overs_per_innings - chaser_overs),  # Avoid 0
        resource_remaining = calculate_resource_remaining(overs_remaining, outcome_by_wickets, fmt),
        resource_used = pmax(0.01, 1 - resource_remaining),  # Avoid division by 0
        projected_final = chaser_score / resource_used,
        implied_rpo = projected_final / overs_per_innings
      ) %>%
      filter(is.finite(projected_final))  # Remove any Inf/NaN rows

    cat(sprintf("--- %s ---\n", toupper(fmt)))

    # Summarize by wickets remaining
    summary <- fmt_data %>%
      group_by(outcome_by_wickets) %>%
      summarise(
        n = n(),
        avg_actual_score = mean(chaser_score, na.rm = TRUE),
        avg_projected_final = mean(projected_final, na.rm = TRUE),
        avg_implied_rpo = mean(implied_rpo, na.rm = TRUE),
        .groups = "drop"
      ) %>%
      filter(n >= 5)

    if (nrow(summary) > 0) {
      # What's a typical run rate for this format?
      typical_rpo <- switch(fmt, "t20" = 8.5, "odi" = 5.5)

      cat(sprintf("%-8s %6s %12s %14s %12s %10s\n",
                  "Wickets", "N", "Actual", "Projected", "Implied RPO", "Expected"))
      cat(paste(rep("-", 65), collapse = ""), "\n")

      for (j in seq_len(nrow(summary))) {
        row <- summary[j, ]
        deviation <- (row$avg_implied_rpo - typical_rpo) / typical_rpo * 100
        cat(sprintf("%-8d %6d %12.0f %14.0f %12.1f %10.1f (%+.0f%%)\n",
                    row$outcome_by_wickets, row$n,
                    row$avg_actual_score, row$avg_projected_final,
                    row$avg_implied_rpo, typical_rpo, deviation))
      }
      cat(sprintf("\nTypical %s RPO: %.1f\n", toupper(fmt), typical_rpo))
      cat("If Implied RPO is much higher/lower than Expected, wicket_value needs adjustment.\n\n")
    }
  }
}

cli::cli_h2("Test 4: Fitting Optimal Wicket Values from Data")

cat("
We can estimate the 'optimal' wicket value by finding what value makes
projected run rates closest to actual format averages.
")

# Get average run rates by format from all completed innings
rpo_query <- "
  SELECT
    CASE
      WHEN m.match_type IN ('T20', 'IT20') THEN 't20'
      WHEN m.match_type IN ('ODI', 'ODM') THEN 'odi'
      WHEN m.match_type IN ('Test', 'MDM') THEN 'test'
    END as format,
    AVG(inn.total_runs / NULLIF(inn.total_overs, 0)) as avg_rpo
  FROM match_innings inn
  JOIN matches m ON inn.match_id = m.match_id
  WHERE inn.total_overs > 0
    AND m.match_type IN ('T20', 'IT20', 'ODI', 'ODM', 'Test', 'MDM')
  GROUP BY format
"

format_rpo <- DBI::dbGetQuery(conn, rpo_query)
cat("\nAverage run rates by format:\n")
for (i in seq_len(nrow(format_rpo))) {
  cat(sprintf("  %s: %.2f runs per over\n", toupper(format_rpo$format[i]), format_rpo$avg_rpo[i]))
}

# Optimize parameters to match DLS benchmark values
cat("\n--- Optimizing parameters to match DLS benchmarks ---\n\n")

# DLS benchmark values (overs_remaining, wickets_remaining, dls_resource%)
# These are from official DLS Standard Edition tables
dls_benchmarks <- list(
  t20 = data.frame(
    overs = c(20, 18, 15, 12, 10, 8, 5, 3, 1,      # 10 wickets
              20, 15, 10, 5, 3,                      # 7 wickets
              20, 15, 10, 5, 3,                      # 5 wickets
              20, 15, 10, 5,                         # 3 wickets
              20, 15, 10, 5, 3),                     # 1 wicket
    wickets = c(10, 10, 10, 10, 10, 10, 10, 10, 10,
                7, 7, 7, 7, 7,
                5, 5, 5, 5, 5,
                3, 3, 3, 3,
                1, 1, 1, 1, 1),
    dls_pct = c(100, 90.3, 77.1, 64.5, 56.7, 47.6, 32.1, 20.6, 7.5,  # 10 wkts
                75.1, 62.7, 52.4, 33.4, 21.0,                         # 7 wkts
                54.4, 46.2, 39.1, 25.7, 16.5,                         # 5 wkts
                34.9, 30.3, 26.1, 17.9,                               # 3 wkts
                8.3, 7.5, 6.6, 4.9, 3.4)                              # 1 wkt
  ),
  odi = data.frame(
    overs = c(50, 45, 40, 35, 30, 25, 20, 15, 10, 5,   # 10 wickets
              50, 40, 30, 20, 10,                        # 7 wickets
              50, 40, 30, 20, 10,                        # 5 wickets
              50, 40, 30, 20, 10,                        # 3 wickets
              50, 40, 30, 20, 10),                       # 1 wicket
    wickets = c(10, 10, 10, 10, 10, 10, 10, 10, 10, 10,
                7, 7, 7, 7, 7,
                5, 5, 5, 5, 5,
                3, 3, 3, 3, 3,
                1, 1, 1, 1, 1),
    dls_pct = c(100, 95.2, 89.3, 82.7, 75.1, 66.5, 56.6, 44.7, 32.1, 17.2,  # 10 wkts
                68.2, 63.0, 57.0, 48.3, 33.4,                                # 7 wkts
                40.0, 38.6, 36.2, 31.6, 22.8,                                # 5 wkts
                20.6, 20.4, 19.9, 18.4, 14.4,                                # 3 wkts
                4.7, 4.7, 4.6, 4.4, 3.8)                                     # 1 wkt
  )
)

# Function to calculate resource with custom parameters
calc_resource_custom <- function(overs, wickets, max_overs, overs_power, wickets_power) {
  overs_pct <- overs / max_overs
  wickets_pct <- wickets / 10
  overs_factor <- overs_pct ^ overs_power
  wickets_factor <- wickets_pct ^ wickets_power
  resource <- overs_factor * wickets_factor
  # Handle edge cases
  resource[wickets == 0] <- 0
  resource[overs == 0] <- 0
  return(resource * 100)
}

# Store optimal parameters
optimal_params <- list()

for (fmt in c("t20", "odi")) {
  bench <- dls_benchmarks[[fmt]]
  max_overs <- switch(fmt, "t20" = 20, "odi" = 50)

  cat(sprintf("=== %s OPTIMIZATION ===\n", toupper(fmt)))

  # Grid search over parameters
  overs_powers <- seq(0.3, 1.5, by = 0.01)
  wickets_powers <- seq(0.5, 2.0, by = 0.01)

  best_rmse <- Inf
  best_op <- NA
  best_wp <- NA

  for (op in overs_powers) {
    for (wp in wickets_powers) {
      pred <- calc_resource_custom(bench$overs, bench$wickets, max_overs, op, wp)
      rmse <- sqrt(mean((pred - bench$dls_pct)^2))
      if (rmse < best_rmse) {
        best_rmse <- rmse
        best_op <- op
        best_wp <- wp
      }
    }
  }

  optimal_params[[fmt]] <- list(overs_power = best_op, wickets_power = best_wp, rmse = best_rmse)

  # Current parameters
  current_op <- switch(fmt, "t20" = 0.82, "odi" = 0.58)
  current_wp <- 1.05
  current_pred <- calc_resource_custom(bench$overs, bench$wickets, max_overs, current_op, current_wp)
  current_rmse <- sqrt(mean((current_pred - bench$dls_pct)^2))

  cat(sprintf("Current:  overs_power=%.2f, wickets_power=%.2f, RMSE=%.2f%%\n",
              current_op, current_wp, current_rmse))
  cat(sprintf("Optimal:  overs_power=%.2f, wickets_power=%.2f, RMSE=%.2f%%\n",
              best_op, best_wp, best_rmse))
  cat(sprintf("Improvement: %.1f%% reduction in RMSE\n\n", (1 - best_rmse/current_rmse) * 100))

  # Show comparison table with optimal parameters
  bench$optimal_pct <- calc_resource_custom(bench$overs, bench$wickets, max_overs, best_op, best_wp)
  bench$opt_error <- bench$optimal_pct - bench$dls_pct

  cat(sprintf("%-12s %8s %10s %10s\n", "Scenario", "DLS%", "Optimal%", "Error"))
  cat(paste(rep("-", 45), collapse = ""), "\n")

  # Show subset of results
  show_idx <- seq(1, nrow(bench), by = max(1, nrow(bench) %/% 12))
  for (i in show_idx) {
    cat(sprintf("%-12s %8.1f %10.1f %+10.1f\n",
                sprintf("%do/%dw", bench$overs[i], bench$wickets[i]),
                bench$dls_pct[i], bench$optimal_pct[i], bench$opt_error[i]))
  }
  cat("\n")
}

# Test format (no official DLS, but we can calibrate based on principles)
cat("=== TEST FORMAT ===\n")
cat("Note: Official DLS tables don't cover Test cricket.\n")
cat("Using principles: overs less constraining, wickets more important.\n")
cat("Recommended: overs_power=0.40, wickets_power=0.85\n")
cat("(Lower overs_power because Tests have ~90 overs/day, time less limiting)\n\n")

optimal_params[["test"]] <- list(overs_power = 0.40, wickets_power = 0.85, rmse = NA)

# Summary
cli::cli_h2("OPTIMAL PARAMETERS SUMMARY")

cat("\nCopy these values to margin_calculation.R:\n\n")
cat("Format   overs_power  wickets_power  RMSE\n")
cat("------   -----------  -------------  ----\n")
for (fmt in c("t20", "odi", "test")) {
  p <- optimal_params[[fmt]]
  rmse_str <- if (is.na(p$rmse)) "N/A" else sprintf("%.2f%%", p$rmse)
  cat(sprintf("%-8s %11.2f  %13.2f  %s\n", toupper(fmt), p$overs_power, p$wickets_power, rmse_str))
}

cli::cli_h2("Recommendation")

cat("
INTERPRETATION:
---------------
- If optimal value is HIGHER than current: We're undervaluing wickets
  (projected scores are too low, margins for wickets wins too small)

- If optimal value is LOWER than current: We're overvaluing wickets
  (projected scores are too high, margins for wickets wins too large)

- If values are close: Current settings are well-calibrated!

NOTE: These estimates depend on data quality and may vary by:
  - Gender (men vs women score at different rates)
  - Era (modern T20s score higher than early ones)
  - Competition level (internationals vs domestic)

Consider running separate optimizations for different categories.
")

DBI::dbDisconnect(conn)

# Part 10: Parameter Sensitivity Analysis ----

cli::cli_h1("Part 10: Parameter Sensitivity Analysis (RMSE Grid)")

cat("
This section shows how RMSE% varies across different parameter combinations.
Lower RMSE = better fit to DLS benchmark values.

The grid helps visualize:
1. How sensitive results are to parameter choices
2. Whether there's a clear optimal region
3. Trade-offs between overs_power and wickets_power
")

cli::cli_h2("RMSE vs Actual Match Data (All Formats)")

cat("
This validates parameters against ACTUAL match outcomes, not just DLS tables.
Method: For wickets wins, compare implied RPO to format average.
If parameters are correct, projected scores should imply realistic run rates.
")

# Reconnect to database
conn <- get_db_connection(read_only = TRUE)

# Get wickets wins for all formats with innings data
all_wickets_query <- "
  SELECT
    m.match_id,
    CASE
      WHEN m.match_type IN ('T20', 'IT20') THEN 't20'
      WHEN m.match_type IN ('ODI', 'ODM') THEN 'odi'
      WHEN m.match_type IN ('Test', 'MDM') THEN 'test'
    END as format,
    m.outcome_by_wickets,
    m.overs_per_innings,
    inn2.total_runs as chaser_score,
    inn2.total_overs as chaser_overs
  FROM matches m
  LEFT JOIN match_innings inn2 ON m.match_id = inn2.match_id AND inn2.innings = 2
  WHERE m.match_type IN ('T20', 'IT20', 'ODI', 'ODM', 'Test', 'MDM')
    AND m.outcome_by_wickets IS NOT NULL
    AND m.outcome_by_wickets > 0
    AND inn2.total_runs IS NOT NULL
    AND inn2.total_overs IS NOT NULL
    AND inn2.total_overs > 5
"

all_wickets_data <- DBI::dbGetQuery(conn, all_wickets_query)
DBI::dbDisconnect(conn)

# Target RPO by format (typical scoring rates)
target_rpo <- list(t20 = 8.5, odi = 5.5, test = 3.2)
max_overs_fmt <- list(t20 = 20, odi = 50, test = 90)

# Function to calculate RMSE vs actual data for any format
calc_actual_rmse <- function(data, format, overs_power, wickets_power) {
  fmt_data <- data[data$format == format, ]
  if (nrow(fmt_data) < 20) return(NA)

  max_overs <- max_overs_fmt[[format]]
  target <- target_rpo[[format]]

  # Calculate overs remaining
  if (format == "test") {
    # For Tests, use 90 overs as reference
    fmt_data$overs_remaining <- pmax(1, 90 - fmt_data$chaser_overs)
  } else {
    # For limited overs, use overs_per_innings
    fmt_data$overs_remaining <- pmax(0.1, fmt_data$overs_per_innings - fmt_data$chaser_overs)
  }

  # Calculate resource remaining
  overs_pct <- fmt_data$overs_remaining / max_overs
  wickets_pct <- fmt_data$outcome_by_wickets / 10
  overs_factor <- overs_pct ^ overs_power
  wickets_factor <- wickets_pct ^ wickets_power

  if (format == "test") {
    resource_remaining <- wickets_factor * (0.4 + 0.6 * overs_factor)
  } else {
    resource_remaining <- overs_factor * wickets_factor
  }

  resource_remaining[fmt_data$outcome_by_wickets == 0] <- 0
  resource_remaining[fmt_data$overs_remaining <= 0] <- 0

  resource_used <- pmax(0.05, 1 - resource_remaining)
  projected_final <- fmt_data$chaser_score / resource_used
  implied_rpo <- projected_final / max_overs

  # Filter extremes
  valid <- is.finite(implied_rpo) & implied_rpo > 0 & implied_rpo < (target * 3)
  if (sum(valid) < 10) return(NA)

  # RMSE as percentage of target
  rmse_pct <- sqrt(mean(((implied_rpo[valid] - target) / target)^2)) * 100
  return(rmse_pct)
}

# Calculate RMSE grids for all formats
cat("\n")

for (fmt in c("t20", "odi", "test")) {
  fmt_data <- all_wickets_data[all_wickets_data$format == fmt, ]
  n_matches <- nrow(fmt_data)

  if (n_matches < 20) {
    cat(sprintf("%s: Insufficient data (%d matches)\n\n", toupper(fmt), n_matches))
    next
  }

  cat(sprintf("=== %s RMSE%% vs Actual Data (N=%d matches, target RPO=%.1f) ===\n\n",
              toupper(fmt), n_matches, target_rpo[[fmt]]))

  # Define parameter ranges based on format
  if (fmt == "t20") {
    op_range <- seq(0.50, 1.00, by = 0.05)
    wp_range <- seq(0.60, 1.20, by = 0.05)
  } else if (fmt == "odi") {
    op_range <- seq(0.40, 0.90, by = 0.05)
    wp_range <- seq(0.80, 1.40, by = 0.05)
  } else {
    op_range <- seq(0.20, 0.70, by = 0.05)
    wp_range <- seq(0.60, 1.10, by = 0.05)
  }

  # Build RMSE matrix
  rmse_matrix <- matrix(NA, nrow = length(wp_range), ncol = length(op_range))

  for (i in seq_along(wp_range)) {
    for (j in seq_along(op_range)) {
      rmse_matrix[i, j] <- calc_actual_rmse(all_wickets_data, fmt, op_range[j], wp_range[i])
    }
  }

  # Determine threshold for optimal region
  threshold <- switch(fmt, "t20" = 15, "odi" = 15, "test" = 20)

  # Print header
  cat(sprintf("%8s", ""))
  for (op in op_range) {
    cat(sprintf(" %6.2f", op))
  }
  cat("\n")
  cat(paste(rep("-", 8 + 7 * length(op_range)), collapse = ""), "\n")

  # Print rows
  for (i in seq_along(wp_range)) {
    cat(sprintf("wp=%.2f |", wp_range[i]))
    for (j in seq_along(op_range)) {
      rmse_val <- rmse_matrix[i, j]
      if (is.na(rmse_val)) {
        cat("     NA")
      } else if (rmse_val < threshold) {
        cat(sprintf(" %5.1f*", rmse_val))
      } else {
        cat(sprintf(" %6.1f", rmse_val))
      }
    }
    cat("\n")
  }

  # Find and report best
  if (any(!is.na(rmse_matrix))) {
    best_idx <- which(rmse_matrix == min(rmse_matrix, na.rm = TRUE), arr.ind = TRUE)
    best_wp <- wp_range[best_idx[1]]
    best_op <- op_range[best_idx[2]]
    best_rmse <- rmse_matrix[best_idx[1], best_idx[2]]

    cat(sprintf("\n%s Best (actual data): overs_power=%.2f, wickets_power=%.2f, RMSE=%.1f%%\n",
                toupper(fmt), best_op, best_wp, best_rmse))

    # Compare to current and DLS-optimized parameters
    current_params <- switch(fmt,
      "t20" = list(op = 0.74, wp = 0.82),
      "odi" = list(op = 0.59, wp = 1.10),
      "test" = list(op = 0.40, wp = 0.85)
    )

    current_rmse <- calc_actual_rmse(all_wickets_data, fmt, current_params$op, current_params$wp)
    linear_rmse <- calc_actual_rmse(all_wickets_data, fmt, 1.0, 1.0)

    cat(sprintf("%s Current (%.2f/%.2f): RMSE=%.1f%%\n",
                toupper(fmt), current_params$op, current_params$wp, current_rmse))
    cat(sprintf("%s Linear (1.0/1.0): RMSE=%.1f%%\n\n", toupper(fmt), linear_rmse))
  }
}

cli::cli_h2("RMSE vs DLS Benchmarks (T20 & ODI only)")

cat("
For comparison, here's RMSE against official DLS tables.
DLS benchmarks are theoretical; actual data shows real-world performance.
")

cli::cli_h3("T20 RMSE Grid (vs DLS benchmarks)")

# Define parameter ranges for grid
overs_powers_grid <- seq(0.5, 1.0, by = 0.05)
wickets_powers_grid <- seq(0.6, 1.2, by = 0.05)

# T20 grid
bench_t20 <- dls_benchmarks[["t20"]]
max_overs_t20 <- 20

# Create RMSE matrix
rmse_matrix_t20 <- matrix(NA, nrow = length(wickets_powers_grid), ncol = length(overs_powers_grid))
rownames(rmse_matrix_t20) <- sprintf("w=%.2f", wickets_powers_grid)
colnames(rmse_matrix_t20) <- sprintf("o=%.2f", overs_powers_grid)

for (i in seq_along(wickets_powers_grid)) {
  for (j in seq_along(overs_powers_grid)) {
    wp <- wickets_powers_grid[i]
    op <- overs_powers_grid[j]
    pred <- calc_resource_custom(bench_t20$overs, bench_t20$wickets, max_overs_t20, op, wp)
    rmse_matrix_t20[i, j] <- sqrt(mean((pred - bench_t20$dls_pct)^2))
  }
}

# Print header
cat("\nT20 RMSE% Grid (rows=wickets_power, cols=overs_power)\n")
cat("Optimal region marked with * (RMSE < 4%)\n\n")

# Print column headers
cat(sprintf("%8s", ""))
for (op in overs_powers_grid) {
  cat(sprintf(" %6.2f", op))
}
cat("\n")
cat(paste(rep("-", 8 + 7 * length(overs_powers_grid)), collapse = ""), "\n")

# Print rows with values
for (i in seq_along(wickets_powers_grid)) {
  cat(sprintf("wp=%.2f |", wickets_powers_grid[i]))
  for (j in seq_along(overs_powers_grid)) {
    rmse_val <- rmse_matrix_t20[i, j]
    if (rmse_val < 4) {
      cat(sprintf(" %5.1f*", rmse_val))
    } else {
      cat(sprintf(" %6.1f", rmse_val))
    }
  }
  cat("\n")
}

# Find and report best
best_idx_t20 <- which(rmse_matrix_t20 == min(rmse_matrix_t20), arr.ind = TRUE)
best_wp_t20 <- wickets_powers_grid[best_idx_t20[1]]
best_op_t20 <- overs_powers_grid[best_idx_t20[2]]
best_rmse_t20 <- rmse_matrix_t20[best_idx_t20[1], best_idx_t20[2]]

cat(sprintf("\nT20 Best in grid: overs_power=%.2f, wickets_power=%.2f, RMSE=%.2f%%\n",
            best_op_t20, best_wp_t20, best_rmse_t20))

cli::cli_h2("ODI RMSE Grid (vs DLS benchmarks)")

# ODI grid
bench_odi <- dls_benchmarks[["odi"]]
max_overs_odi <- 50

# Different ranges for ODI (wickets_power tends to be higher)
overs_powers_grid_odi <- seq(0.4, 0.9, by = 0.05)
wickets_powers_grid_odi <- seq(0.8, 1.4, by = 0.05)

rmse_matrix_odi <- matrix(NA, nrow = length(wickets_powers_grid_odi), ncol = length(overs_powers_grid_odi))
rownames(rmse_matrix_odi) <- sprintf("w=%.2f", wickets_powers_grid_odi)
colnames(rmse_matrix_odi) <- sprintf("o=%.2f", overs_powers_grid_odi)

for (i in seq_along(wickets_powers_grid_odi)) {
  for (j in seq_along(overs_powers_grid_odi)) {
    wp <- wickets_powers_grid_odi[i]
    op <- overs_powers_grid_odi[j]
    pred <- calc_resource_custom(bench_odi$overs, bench_odi$wickets, max_overs_odi, op, wp)
    rmse_matrix_odi[i, j] <- sqrt(mean((pred - bench_odi$dls_pct)^2))
  }
}

# Print header
cat("\nODI RMSE% Grid (rows=wickets_power, cols=overs_power)\n")
cat("Optimal region marked with * (RMSE < 5%)\n\n")

# Print column headers
cat(sprintf("%8s", ""))
for (op in overs_powers_grid_odi) {
  cat(sprintf(" %6.2f", op))
}
cat("\n")
cat(paste(rep("-", 8 + 7 * length(overs_powers_grid_odi)), collapse = ""), "\n")

# Print rows with values
for (i in seq_along(wickets_powers_grid_odi)) {
  cat(sprintf("wp=%.2f |", wickets_powers_grid_odi[i]))
  for (j in seq_along(overs_powers_grid_odi)) {
    rmse_val <- rmse_matrix_odi[i, j]
    if (rmse_val < 5) {
      cat(sprintf(" %5.1f*", rmse_val))
    } else {
      cat(sprintf(" %6.1f", rmse_val))
    }
  }
  cat("\n")
}

# Find and report best
best_idx_odi <- which(rmse_matrix_odi == min(rmse_matrix_odi), arr.ind = TRUE)
best_wp_odi <- wickets_powers_grid_odi[best_idx_odi[1]]
best_op_odi <- overs_powers_grid_odi[best_idx_odi[2]]
best_rmse_odi <- rmse_matrix_odi[best_idx_odi[1], best_idx_odi[2]]

cat(sprintf("\nODI Best in grid: overs_power=%.2f, wickets_power=%.2f, RMSE=%.2f%%\n",
            best_op_odi, best_wp_odi, best_rmse_odi))

cli::cli_h2("Test Format Parameter Analysis")

cat("
Note: No official DLS benchmark exists for Test cricket.
Instead, we analyze how parameters affect resource values at key scenarios.

Test cricket principles:
- Overs are LESS constraining (90+ overs/day, can declare)
- Wickets are MORE important (losing wickets = losing match)
- Time pressure varies (5th day draw attempts vs chasing targets)
")

# Test format - show resource values for different parameter combinations
# Key scenarios for Test cricket
test_scenarios <- data.frame(
  overs = c(90, 60, 30, 15, 90, 60, 30, 15, 90, 60, 30, 15),
  wickets = c(10, 10, 10, 10, 5, 5, 5, 5, 2, 2, 2, 2),
  desc = c("Full day/10w", "40 overs gone/10w", "60 overs gone/10w", "75 overs gone/10w",
           "Full day/5w", "40 overs gone/5w", "60 overs gone/5w", "75 overs gone/5w",
           "Full day/2w", "40 overs gone/2w", "60 overs gone/2w", "75 overs gone/2w")
)

# Test different parameter combinations
test_params <- list(
  list(name = "Current (0.40/0.85)", op = 0.40, wp = 0.85),
  list(name = "ODI-like (0.59/1.10)", op = 0.59, wp = 1.10),
  list(name = "Wicket-heavy (0.30/0.70)", op = 0.30, wp = 0.70),
  list(name = "Balanced (0.50/0.90)", op = 0.50, wp = 0.90),
  list(name = "Linear (1.0/1.0)", op = 1.0, wp = 1.0)
)

max_overs_test <- 90

cat("\nResource % for different Test parameters:\n\n")

# Print header
cat(sprintf("%-20s", "Scenario"))
for (p in test_params) {
  cat(sprintf(" %12s", substr(p$name, 1, 12)))
}
cat("\n")
cat(paste(rep("-", 20 + 13 * length(test_params)), collapse = ""), "\n")

# Print each scenario
for (i in seq_len(nrow(test_scenarios))) {
  cat(sprintf("%-20s", test_scenarios$desc[i]))
  for (p in test_params) {
    # Use Test-specific formula: wickets_factor * (0.4 + 0.6 * overs_factor)
    overs_pct <- test_scenarios$overs[i] / max_overs_test
    wickets_pct <- test_scenarios$wickets[i] / 10
    overs_factor <- overs_pct ^ p$op
    wickets_factor <- wickets_pct ^ p$wp
    resource <- wickets_factor * (0.4 + 0.6 * overs_factor) * 100
    if (test_scenarios$wickets[i] == 0) resource <- 0
    cat(sprintf(" %11.1f%%", resource))
  }
  cat("\n")
}

cat("
Test Parameter Recommendations:
- overs_power should be LOW (0.30-0.50) because time is rarely the constraint
- wickets_power should be ~0.70-0.90 (concave - early wickets hurt more)
- The formula uses: resource = wickets_factor * (0.4 + 0.6 * overs_factor)
  This ensures wickets dominate but overs still matter somewhat

Current choice: overs_power=0.40, wickets_power=0.85
")

cli::cli_h2("RMSE Summary Table")

cat("\nComparison of parameter choices:\n\n")
cat(sprintf("%-8s %-20s %8s %8s %10s\n", "Format", "Parameters", "OP", "WP", "RMSE%"))
cat(paste(rep("-", 58), collapse = ""), "\n")

# T20 comparisons
t20_configs <- list(
  list(name = "Grid optimal", op = best_op_t20, wp = best_wp_t20),
  list(name = "Fine-tuned optimal", op = 0.74, wp = 0.82),
  list(name = "Original (linear)", op = 1.0, wp = 1.0),
  list(name = "Conservative", op = 0.80, wp = 0.90),
  list(name = "Aggressive", op = 0.65, wp = 0.75)
)

for (cfg in t20_configs) {
  pred <- calc_resource_custom(bench_t20$overs, bench_t20$wickets, max_overs_t20, cfg$op, cfg$wp)
  rmse <- sqrt(mean((pred - bench_t20$dls_pct)^2))
  cat(sprintf("%-8s %-20s %8.2f %8.2f %9.2f%%\n", "T20", cfg$name, cfg$op, cfg$wp, rmse))
}

cat("\n")

# ODI comparisons
odi_configs <- list(
  list(name = "Grid optimal", op = best_op_odi, wp = best_wp_odi),
  list(name = "Fine-tuned optimal", op = 0.59, wp = 1.10),
  list(name = "Original (linear)", op = 1.0, wp = 1.0),
  list(name = "Conservative", op = 0.70, wp = 1.00),
  list(name = "Aggressive", op = 0.50, wp = 1.20)
)

for (cfg in odi_configs) {
  pred <- calc_resource_custom(bench_odi$overs, bench_odi$wickets, max_overs_odi, cfg$op, cfg$wp)
  rmse <- sqrt(mean((pred - bench_odi$dls_pct)^2))
  cat(sprintf("%-8s %-20s %8.2f %8.2f %9.2f%%\n", "ODI", cfg$name, cfg$op, cfg$wp, rmse))
}

cat("
KEY INSIGHTS:
1. T20: Lower overs_power (~0.74) and wickets_power (~0.82) work best
   - First overs/wickets are more valuable than later ones
   - Concave relationship (diminishing returns)

2. ODI: Lower overs_power (~0.59) but higher wickets_power (~1.10)
   - Overs even more front-loaded in 50-over cricket
   - Wickets have slightly convex relationship (later wickets more costly)

3. Original linear model (power=1.0) consistently worse
   - RMSE 5-8% vs 4% for optimized parameters
   - Linear relationship doesn't match DLS behavior

4. Parameters are robust - small changes don't dramatically affect RMSE
   - Any values in the 'optimal region' (*) perform similarly
")

cli::cli_h1("Summary")

cat("
KEY TAKEAWAYS:

1. RUNS WINS: Simple difference (team1 - team2)

2. WICKETS WINS: Project what chaser 'would have scored'
   - More wickets remaining = higher projected score = bigger margin
   - More overs remaining = higher projected score = bigger margin

3. DRAWS: margin = 0 (no ELO impact from result)

4. MOV MULTIPLIER (G-factor):
   - Scales ELO updates based on margin size
   - Dampens expected results (favorite wins)
   - Amplifies upsets (underdog wins)
   - Clamped between MOV_MIN and MOV_MAX

5. TEST vs LIMITED OVERS:
   - Tests have higher wicket values and more overs
   - Same 'by X wickets' can mean very different unified margins
   - Test margins tend to be larger in absolute terms
")

cli::cli_alert_success("MOV explanation complete!")
