# Debug 3-Way ELO Rankings
# Run this in RStudio to check top/bottom entities for each ELO type

library(DBI)
library(duckdb)
library(data.table)

# Connect (use existing connection if available, or create new read-only)
if (!exists("conn") || !DBI::dbIsValid(conn)) {
  db_path <- file.path(dirname(getwd()), "bouncerdata", "bouncer.duckdb")
  conn <- DBI::dbConnect(duckdb::duckdb(), db_path, read_only = TRUE)
}

# Helper to get latest ELO for each entity
cat("\n=== 3-WAY ELO RANKINGS DEBUG ===\n\n")

# 1. BATTER RUN ELO (Top & Bottom 10)
cat("═══════════════════════════════════════════════════════════════\n")
cat("1. BATTER RUN ELO - Who scores runs relative to expected?\n")
cat("   High = consistently scores more than expected\n")
cat("═══════════════════════════════════════════════════════════════\n\n")

batter_run_elo <- DBI::dbGetQuery(conn, "
  WITH latest AS (
    SELECT batter_id, batter_run_elo_after as elo,
           ROW_NUMBER() OVER (PARTITION BY batter_id ORDER BY delivery_id DESC) as rn
    FROM t20_3way_elo
  )
  SELECT l.batter_id, p.player_name, l.elo
  FROM latest l
  LEFT JOIN players p ON l.batter_id = p.player_id
  WHERE rn = 1
  ORDER BY elo DESC
")

cat("TOP 10 Batters (Run ELO):\n")
print(head(batter_run_elo, 10))
cat("\nBOTTOM 10 Batters (Run ELO):\n")
print(tail(batter_run_elo, 10))

# 2. BOWLER RUN ELO (Top & Bottom 10)
cat("\n═══════════════════════════════════════════════════════════════\n")
cat("2. BOWLER RUN ELO - Who restricts runs relative to expected?\n")
cat("   High = consistently concedes fewer runs than expected\n")
cat("═══════════════════════════════════════════════════════════════\n\n")

bowler_run_elo <- DBI::dbGetQuery(conn, "
  WITH latest AS (
    SELECT bowler_id, bowler_run_elo_after as elo,
           ROW_NUMBER() OVER (PARTITION BY bowler_id ORDER BY delivery_id DESC) as rn
    FROM t20_3way_elo
  )
  SELECT l.bowler_id, p.player_name, l.elo
  FROM latest l
  LEFT JOIN players p ON l.bowler_id = p.player_id
  WHERE rn = 1
  ORDER BY elo DESC
")

cat("TOP 10 Bowlers (Run ELO):\n")
print(head(bowler_run_elo, 10))
cat("\nBOTTOM 10 Bowlers (Run ELO):\n")
print(tail(bowler_run_elo, 10))

# 3. BATTER WICKET ELO (Top & Bottom 10)
cat("\n═══════════════════════════════════════════════════════════════\n")
cat("3. BATTER WICKET ELO - Who avoids getting out?\n")
cat("   High = harder to dismiss than expected\n")
cat("═══════════════════════════════════════════════════════════════\n\n")

batter_wicket_elo <- DBI::dbGetQuery(conn, "
  WITH latest AS (
    SELECT batter_id, batter_wicket_elo_after as elo,
           ROW_NUMBER() OVER (PARTITION BY batter_id ORDER BY delivery_id DESC) as rn
    FROM t20_3way_elo
  )
  SELECT l.batter_id, p.player_name, l.elo
  FROM latest l
  LEFT JOIN players p ON l.batter_id = p.player_id
  WHERE rn = 1
  ORDER BY elo DESC
")

cat("TOP 10 Batters (Wicket ELO - hardest to dismiss):\n")
print(head(batter_wicket_elo, 10))
cat("\nBOTTOM 10 Batters (Wicket ELO - easiest to dismiss):\n")
print(tail(batter_wicket_elo, 10))

# 4. BOWLER WICKET ELO (Top & Bottom 10)
cat("\n═══════════════════════════════════════════════════════════════\n")
cat("4. BOWLER WICKET ELO - Who takes wickets?\n")
cat("   High = takes more wickets than expected\n")
cat("═══════════════════════════════════════════════════════════════\n\n")

bowler_wicket_elo <- DBI::dbGetQuery(conn, "
  WITH latest AS (
    SELECT bowler_id, bowler_wicket_elo_after as elo,
           ROW_NUMBER() OVER (PARTITION BY bowler_id ORDER BY delivery_id DESC) as rn
    FROM t20_3way_elo
  )
  SELECT l.bowler_id, p.player_name, l.elo
  FROM latest l
  LEFT JOIN players p ON l.bowler_id = p.player_id
  WHERE rn = 1
  ORDER BY elo DESC
")

cat("TOP 10 Bowlers (Wicket ELO):\n")
print(head(bowler_wicket_elo, 10))
cat("\nBOTTOM 10 Bowlers (Wicket ELO):\n")
print(tail(bowler_wicket_elo, 10))

# 5. VENUE PERMANENT RUN ELO (Top & Bottom 10)
cat("\n═══════════════════════════════════════════════════════════════\n")
cat("5. VENUE PERMANENT RUN ELO - Which venues favor run scoring?\n")
cat("   High = consistently higher scoring venue\n")
cat("═══════════════════════════════════════════════════════════════\n\n")

venue_perm_run_elo <- DBI::dbGetQuery(conn, "
  WITH latest AS (
    SELECT venue, venue_perm_run_elo_after as elo,
           ROW_NUMBER() OVER (PARTITION BY venue ORDER BY delivery_id DESC) as rn
    FROM t20_3way_elo
  )
  SELECT venue, elo
  FROM latest
  WHERE rn = 1
  ORDER BY elo DESC
")

cat("TOP 10 Venues (High-scoring):\n")
print(head(venue_perm_run_elo, 10))
cat("\nBOTTOM 10 Venues (Low-scoring):\n")
print(tail(venue_perm_run_elo, 10))

# 6. VENUE PERMANENT WICKET ELO (Top & Bottom 10)
cat("\n═══════════════════════════════════════════════════════════════\n")
cat("6. VENUE PERMANENT WICKET ELO - Which venues take more wickets?\n")
cat("   High = wickets fall more often than expected\n")
cat("═══════════════════════════════════════════════════════════════\n\n")

venue_perm_wicket_elo <- DBI::dbGetQuery(conn, "
  WITH latest AS (
    SELECT venue, venue_perm_wicket_elo_after as elo,
           ROW_NUMBER() OVER (PARTITION BY venue ORDER BY delivery_id DESC) as rn
    FROM t20_3way_elo
  )
  SELECT venue, elo
  FROM latest
  WHERE rn = 1
  ORDER BY elo DESC
")

cat("TOP 10 Venues (Wicket-taking):\n")
print(head(venue_perm_wicket_elo, 10))
cat("\nBOTTOM 10 Venues (Batting-safe):\n")
print(tail(venue_perm_wicket_elo, 10))

# 7. VENUE SESSION ELOs (Distribution check - these reset each match)
cat("\n═══════════════════════════════════════════════════════════════\n")
cat("7. VENUE SESSION ELOs - Within-match condition adaptation\n")
cat("   These reset each match, so checking distribution\n")
cat("═══════════════════════════════════════════════════════════════\n\n")

session_stats <- DBI::dbGetQuery(conn, "
  SELECT
    'Session Run ELO' as elo_type,
    MIN(venue_session_run_elo_after) as min_elo,
    AVG(venue_session_run_elo_after) as mean_elo,
    MAX(venue_session_run_elo_after) as max_elo,
    STDDEV(venue_session_run_elo_after) as sd_elo
  FROM t20_3way_elo
  UNION ALL
  SELECT
    'Session Wicket ELO' as elo_type,
    MIN(venue_session_wicket_elo_after) as min_elo,
    AVG(venue_session_wicket_elo_after) as mean_elo,
    MAX(venue_session_wicket_elo_after) as max_elo,
    STDDEV(venue_session_wicket_elo_after) as sd_elo
  FROM t20_3way_elo
")

cat("Session ELO Distribution (resets each match, adapts to day conditions):\n")
print(session_stats)

# 8. VENUE RUN vs WICKET CORRELATION CHECK
cat("\n═══════════════════════════════════════════════════════════════\n")
cat("8. VENUE RUN vs WICKET ELO - Are they independent?\n")
cat("   Shows that run scoring and wicket taking are separate characteristics\n")
cat("═══════════════════════════════════════════════════════════════\n\n")

venue_comparison <- DBI::dbGetQuery(conn, "
  WITH latest_run AS (
    SELECT venue, venue_perm_run_elo_after as run_elo,
           ROW_NUMBER() OVER (PARTITION BY venue ORDER BY delivery_id DESC) as rn
    FROM t20_3way_elo
  ),
  latest_wicket AS (
    SELECT venue, venue_perm_wicket_elo_after as wicket_elo,
           ROW_NUMBER() OVER (PARTITION BY venue ORDER BY delivery_id DESC) as rn
    FROM t20_3way_elo
  )
  SELECT r.venue, r.run_elo, w.wicket_elo,
         (r.run_elo - 1500) as run_diff,
         (w.wicket_elo - 1500) as wicket_diff
  FROM latest_run r
  JOIN latest_wicket w ON r.venue = w.venue
  WHERE r.rn = 1 AND w.rn = 1
  ORDER BY r.run_elo DESC
")

cat("Correlation between Run ELO and Wicket ELO:\n")
cat(sprintf("  r = %.3f\n\n", cor(venue_comparison$run_elo, venue_comparison$wicket_elo)))

cat("Interesting cases (high runs but low wickets, or vice versa):\n\n")

# High run, low wicket (batting paradise - hard to get out)
cat("High scoring but hard to get out (batting paradise):\n")
batting_paradise <- venue_comparison[venue_comparison$run_diff > 30 & venue_comparison$wicket_diff < -10, ]
print(head(batting_paradise, 5))

# Low run, high wicket (bowler friendly)
cat("\nLow scoring AND wickets fall (bowler's dream):\n")
bowler_friendly <- venue_comparison[venue_comparison$run_diff < -30 & venue_comparison$wicket_diff > 10, ]
print(head(bowler_friendly, 5))

# Summary table
cat("\n═══════════════════════════════════════════════════════════════\n")
cat("SUMMARY: Does this make sense?\n")
cat("═══════════════════════════════════════════════════════════════\n\n")

cat("Check these against cricket knowledge:\n")
cat("- Top batter run ELOs should be elite T20 batters (Kohli, Gayle, ABD, etc.)\n")
cat("- Top bowler run ELOs should be economical bowlers\n")
cat("- Top batter wicket ELOs should be hard-to-dismiss players\n")
cat("- Top bowler wicket ELOs should be wicket-takers (Malinga, Rashid, etc.)\n")
cat("- High-scoring grounds: small boundaries, flat pitches\n")
cat("- Low-scoring grounds: seamer-friendly, larger grounds\n")
cat("- If correlation between venue run/wicket ELOs is low (<0.5), the separate tracking is valuable\n")
