# Graph-Based ELO Calibration System
# Uses player interaction network to propagate calibration from anchor players

library(duckdb)
library(DBI)

conn <- dbConnect(duckdb(), "C:/Users/peteo/OneDrive/Documents/bouncerverse/bouncerdata/bouncer.duckdb", read_only = TRUE)

cat("=== GRAPH-BASED ELO CALIBRATION ===\n\n")

# ==============================================================================
# STEP 1: Define Strict Anchor Events
# ==============================================================================
cat("--- Step 1: Define Strict Anchor Events ---\n")

anchor_pattern <- paste(
  "Indian Premier League",
  "Big Bash League",
  "Pakistan Super League",
  "Caribbean Premier League",
  "SA20",
  "The Hundred Men",
  "The Hundred Women",
  "Major League Cricket",
  sep = "|"
)

# Also include actual T20 World Cup (not qualifiers)
wc_pattern <- "ICC Men's T20 World Cup$|ICC Women's T20 World Cup$"

cat("Anchor leagues:", anchor_pattern, "\n")
cat("Plus World Cup main events (not qualifiers)\n\n")

# ==============================================================================
# STEP 2: Compute Player Anchor Exposure
# ==============================================================================
cat("--- Step 2: Compute Player Anchor Exposure ---\n")

# Get anchor exposure for all players (as both batter and bowler)
player_anchor <- dbGetQuery(conn, "
WITH player_balls AS (
  -- As batter
  SELECT
    e.batter_id as player_id,
    'batter' as role,
    COUNT(*) as total_balls,
    SUM(CASE
      WHEN m.event_name LIKE '%Indian Premier League%'
        OR m.event_name LIKE '%Big Bash League%'
        OR m.event_name LIKE '%Pakistan Super League%'
        OR m.event_name LIKE '%Caribbean Premier League%'
        OR m.event_name LIKE '%SA20%'
        OR m.event_name LIKE '%The Hundred Men%'
        OR m.event_name LIKE '%The Hundred Women%'
        OR m.event_name LIKE '%Major League Cricket%'
        OR (m.event_name LIKE '%T20 World Cup%' AND m.event_name NOT LIKE '%Qualifier%')
      THEN 1 ELSE 0 END) as anchor_balls
  FROM t20_3way_elo e
  JOIN cricsheet.matches m ON e.match_id = m.match_id
  GROUP BY e.batter_id

  UNION ALL

  -- As bowler
  SELECT
    e.bowler_id as player_id,
    'bowler' as role,
    COUNT(*) as total_balls,
    SUM(CASE
      WHEN m.event_name LIKE '%Indian Premier League%'
        OR m.event_name LIKE '%Big Bash League%'
        OR m.event_name LIKE '%Pakistan Super League%'
        OR m.event_name LIKE '%Caribbean Premier League%'
        OR m.event_name LIKE '%SA20%'
        OR m.event_name LIKE '%The Hundred Men%'
        OR m.event_name LIKE '%The Hundred Women%'
        OR m.event_name LIKE '%Major League Cricket%'
        OR (m.event_name LIKE '%T20 World Cup%' AND m.event_name NOT LIKE '%Qualifier%')
      THEN 1 ELSE 0 END) as anchor_balls
  FROM t20_3way_elo e
  JOIN cricsheet.matches m ON e.match_id = m.match_id
  GROUP BY e.bowler_id
)
SELECT
  player_id,
  SUM(total_balls) as total_balls,
  SUM(anchor_balls) as anchor_balls,
  ROUND(SUM(anchor_balls) * 100.0 / SUM(total_balls), 2) as anchor_pct
FROM player_balls
GROUP BY player_id
")

cat("Total players:", nrow(player_anchor), "\n")
cat("Players with >0%% anchor:", sum(player_anchor$anchor_pct > 0), "\n")
cat("Players with >10%% anchor:", sum(player_anchor$anchor_pct > 10), "\n")
cat("Players with >50%% anchor:", sum(player_anchor$anchor_pct > 50), "\n\n")

# ==============================================================================
# STEP 3: Build Player Interaction Graph
# ==============================================================================
cat("--- Step 3: Build Player Interaction Graph ---\n")

# Get all batter-bowler interactions with ball counts
interactions <- dbGetQuery(conn, "
SELECT
  batter_id,
  bowler_id,
  COUNT(*) as balls
FROM t20_3way_elo
GROUP BY batter_id, bowler_id
")

cat("Total batter-bowler pairs:", nrow(interactions), "\n\n")

# ==============================================================================
# STEP 4: Compute Indirect Anchor Exposure (Opposition Quality)
# ==============================================================================
cat("--- Step 4: Compute Indirect Anchor Exposure ---\n")

# For each batter, compute weighted average anchor exposure of bowlers faced
batter_opp_quality <- merge(
  interactions,
  player_anchor[, c("player_id", "anchor_pct")],
  by.x = "bowler_id",
  by.y = "player_id",
  all.x = TRUE
)
batter_opp_quality$anchor_pct[is.na(batter_opp_quality$anchor_pct)] <- 0

batter_indirect <- aggregate(
  cbind(weighted_anchor = anchor_pct * balls, total_balls = balls) ~ batter_id,
  data = batter_opp_quality,
  FUN = sum
)
batter_indirect$opp_anchor_pct <- batter_indirect$weighted_anchor / batter_indirect$total_balls

# For each bowler, compute weighted average anchor exposure of batters faced
bowler_opp_quality <- merge(
  interactions,
  player_anchor[, c("player_id", "anchor_pct")],
  by.x = "batter_id",
  by.y = "player_id",
  all.x = TRUE
)
bowler_opp_quality$anchor_pct[is.na(bowler_opp_quality$anchor_pct)] <- 0

bowler_indirect <- aggregate(
  cbind(weighted_anchor = anchor_pct * balls, total_balls = balls) ~ bowler_id,
  data = bowler_opp_quality,
  FUN = sum
)
bowler_indirect$opp_anchor_pct <- bowler_indirect$weighted_anchor / bowler_indirect$total_balls

# Combine batter and bowler indirect exposure
player_indirect <- rbind(
  data.frame(player_id = batter_indirect$batter_id, opp_anchor_pct = batter_indirect$opp_anchor_pct),
  data.frame(player_id = bowler_indirect$bowler_id, opp_anchor_pct = bowler_indirect$opp_anchor_pct)
)
player_indirect <- aggregate(opp_anchor_pct ~ player_id, data = player_indirect, FUN = max)

cat("Computed indirect anchor exposure for", nrow(player_indirect), "players\n\n")

# ==============================================================================
# STEP 5: Compute Calibration Score
# ==============================================================================
cat("--- Step 5: Compute Calibration Score ---\n")

# Merge direct and indirect exposure
calibration <- merge(player_anchor, player_indirect, by = "player_id", all = TRUE)
calibration$anchor_pct[is.na(calibration$anchor_pct)] <- 0
calibration$opp_anchor_pct[is.na(calibration$opp_anchor_pct)] <- 0

# Calibration score = weighted combination of direct and indirect exposure
# Direct exposure matters more (you've been tested)
# Indirect exposure matters when direct is low (your opponents have been tested)
calibration$calibration_score <- pmin(100,
  calibration$anchor_pct * 0.7 +  # Direct contribution
  calibration$opp_anchor_pct * 0.3  # Indirect contribution
)

cat("Calibration score distribution:\n")
cat("  0-5:", sum(calibration$calibration_score <= 5), "\n")
cat("  5-20:", sum(calibration$calibration_score > 5 & calibration$calibration_score <= 20), "\n")
cat("  20-50:", sum(calibration$calibration_score > 20 & calibration$calibration_score <= 50), "\n")
cat("  50+:", sum(calibration$calibration_score > 50), "\n\n")

# ==============================================================================
# STEP 6: Compute ELO Adjustments
# ==============================================================================
cat("--- Step 6: Compute ELO Adjustments ---\n")

# Get current ELOs
current_elos <- dbGetQuery(conn, "
WITH latest_batter AS (
  SELECT batter_id as player_id, batter_run_elo_after as elo,
         ROW_NUMBER() OVER (PARTITION BY batter_id ORDER BY match_date DESC) as rn
  FROM t20_3way_elo
),
latest_bowler AS (
  SELECT bowler_id as player_id, bowler_wicket_elo_after as elo,
         ROW_NUMBER() OVER (PARTITION BY bowler_id ORDER BY match_date DESC) as rn
  FROM t20_3way_elo
)
SELECT player_id, MAX(elo) as current_elo
FROM (
  SELECT player_id, elo FROM latest_batter WHERE rn = 1
  UNION ALL
  SELECT player_id, elo FROM latest_bowler WHERE rn = 1
) combined
GROUP BY player_id
")

# Merge with calibration
final <- merge(calibration, current_elos, by = "player_id", all.x = TRUE)

# Add player names
player_names <- dbGetQuery(conn, "SELECT player_id, player_name FROM cricsheet.players")
final <- merge(final, player_names, by = "player_id", all.x = TRUE)

# Compute adjustment:
# - High calibration score (>50): No adjustment
# - Low calibration score (<5): Pull heavily toward replacement (1300)
# - In between: Linear interpolation

replacement_level <- 1300
final$adjustment_factor <- pmin(1, final$calibration_score / 50)  # 0 to 1

# Adjusted ELO = blend of current ELO and replacement level
final$adjusted_elo <- final$current_elo * final$adjustment_factor +
                      replacement_level * (1 - final$adjustment_factor)

final$elo_change <- final$adjusted_elo - final$current_elo

cat("\n=== TOP 20 BY CURRENT ELO ===\n")
top_current <- final[order(-final$current_elo), ][1:20, ]
print(top_current[, c("player_name", "current_elo", "anchor_pct", "opp_anchor_pct",
                      "calibration_score", "adjusted_elo", "elo_change")])

cat("\n=== TOP 20 BY ADJUSTED ELO ===\n")
top_adjusted <- final[order(-final$adjusted_elo), ][1:20, ]
print(top_adjusted[, c("player_name", "current_elo", "anchor_pct", "opp_anchor_pct",
                       "calibration_score", "adjusted_elo", "elo_change")])

cat("\n=== BIGGEST DOWNWARD ADJUSTMENTS ===\n")
biggest_down <- final[order(final$elo_change), ][1:20, ]
print(biggest_down[, c("player_name", "current_elo", "anchor_pct", "opp_anchor_pct",
                       "calibration_score", "adjusted_elo", "elo_change")])

cat("\n=== ADIL BUTT SPECIFICALLY ===\n")
adil <- final[final$player_id == "fde53ec1", ]
print(adil[, c("player_name", "current_elo", "anchor_pct", "opp_anchor_pct",
               "calibration_score", "adjusted_elo", "elo_change")])

cat("\n=== CALIBRATION FORMULA ===\n")
cat("
calibration_score = 0.7 * direct_anchor_pct + 0.3 * opponent_anchor_pct
adjustment_factor = min(1, calibration_score / 50)
adjusted_elo = current_elo * adjustment_factor + 1300 * (1 - adjustment_factor)

Interpretation:
- calibration_score >= 50: Full trust in current ELO
- calibration_score = 25: 50% trust, 50% pulled to replacement
- calibration_score = 0: 100% pulled to replacement level (1300)
")

# ==============================================================================
# STEP 7: Save calibration data
# ==============================================================================
cat("\n--- Saving calibration data ---\n")
output_path <- "C:/Users/peteo/OneDrive/Documents/bouncerverse/bouncerdata/temp_3way_elo/player_calibration.csv"
write.csv(final[, c("player_id", "player_name", "total_balls", "anchor_pct",
                    "opp_anchor_pct", "calibration_score", "current_elo",
                    "adjusted_elo", "elo_change")],
          output_path, row.names = FALSE)
cat("Saved to:", output_path, "\n")

dbDisconnect(conn, shutdown = TRUE)
