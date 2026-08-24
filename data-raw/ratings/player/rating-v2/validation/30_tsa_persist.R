# Persist TSA into main.cricsheet_ball_raa for every limited-overs bucket,
# BOTH innings.
#
# This script used to do innings 1 only, on the reasoning that "a chase is
# truncated the moment the target is passed, so projected final score stops
# being the quantity the model predicts". That does not survive contact with
# the formula. TSA is a DIFFERENCE of two projections taken at the SAME ball --
# what the projection becomes given the actual outcome, minus what it becomes
# given the expected outcome. calculate_projected_scores_vectorized() projects
# the full ball allocation regardless of what happened next, which IS the
# adjusted (un-truncated) score. Truncation removes ROWS from the tail of a
# chase; it does not corrupt the rows that exist, and both terms of the
# difference are affected identically anyway.
#
# The genuine chase-specific worry is intent: once the target is secure batters
# stop trying to score and the agnostic model does not know that, so actual
# falls below expected on purpose. Measured rather than assumed -- see the
# by-band table below. It turns out to be negligible because the band barely
# exists: an innings ENDS when the target is passed, so "already won" is 48-240
# balls per bucket out of hundreds of thousands.
#
# Adding innings 2 took limited-overs coverage from ~54% to ~100% and roughly
# doubled the rows available to the TSA rating (bouncerverse#61 arm 5).
#
# TEST STILL GETS NO TSA, and the adjusted score does not fix that: a Test
# innings has no fixed ball allocation, so "projected final score" has no
# denominator to project against. That needs a different construct entirely
# (expected remaining runs given wickets in hand), not a variation on this one.
suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
DB <- file.path(find_bouncerdata_dir(), "bouncer.duckdb")
stopifnot(file.exists(DB), file.info(DB)$size > 1e10)
conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = DB, read_only = FALSE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

have <- DBI::dbGetQuery(conn, "
  SELECT column_name FROM information_schema.columns
  WHERE table_schema='main' AND table_name='cricsheet_ball_raa'")$column_name
if (!"tsa" %in% have) {
  DBI::dbExecute(conn, "ALTER TABLE main.cricsheet_ball_raa ADD COLUMN tsa DOUBLE")
  cat("added column tsa\n")
}

buckets <- list(list(f="t20", g="male", mb=120L), list(f="odi", g="male", mb=300L),
                list(f="t20", g="female", mb=120L), list(f="odi", g="female", mb=300L))

total <- 0L
for (bk in buckets) for (INN in 1:2) {
  FMT <- bk$f; GEN <- bk$g; MAXB <- bk$mb
  p <- tryCatch(load_projection_params(FMT, GEN, "international", conn = conn),
                error = function(e) NULL)
  if (is.null(p)) { cat(sprintf("  %s %s: no projection params, skipped\n", FMT, GEN)); next }

  d <- as.data.table(DBI::dbGetQuery(conn, sprintf("
    SELECT r.delivery_id, r.over_number, r.ball_number, r.actual_runs,
           CAST(r.is_wicket AS INT) AS is_wicket, r.exp_runs, r.exp_wicket,
           d.total_runs - d.runs_total AS runs_pre,
           d.wickets_fallen - CAST(d.is_wicket AS INT) AS wkts_pre
    FROM main.cricsheet_ball_raa r
    JOIN cricsheet.deliveries d ON d.delivery_id = r.delivery_id
    WHERE r.format='%s' AND r.gender='%s' AND r.innings_number = %d",
    toupper(FMT), GEN, INN)))
  if (!nrow(d)) { cat(sprintf("  %s %s inn%d: no rows\n", FMT, GEN, INN)); next }

  # FRAME CHECK. Every projection below assumes total_runs restarts each
  # innings. If it were cumulative across the match, innings-2 runs_pre would
  # open near the first-innings total and every projection would be nonsense --
  # while still producing entirely plausible-looking numbers.
  #
  # ball_number is 1-BASED. The first version of this check tested for ball 0,
  # matched zero rows, and reported "n/a"; a guard that cannot fail is not a
  # guard, and it would have waved a match-cumulative score straight through.
  opener <- d[over_number == 0L & ball_number == 1L]
  if (!nrow(opener)) {
    stop(sprintf("%s %s inn%d: no opening ball found, frame unverified", FMT, GEN, INN))
  }
  if (max(opener$runs_pre) > 0) {
    stop(sprintf("%s %s inn%d: score does not restart (max runs_pre %s on the opening ball)",
                 FMT, GEN, INN, max(opener$runs_pre)))
  }

  d[, bb := over_number * 6L + ball_number]
  pr <- function(sc, wr, br) calculate_projected_scores_vectorized(
    current_score = sc, wickets_remaining = wr, balls_remaining = br,
    expected_initial_score = p$eis_agnostic, a = p$a, b = p$b, z = p$z, y = p$y,
    max_balls = MAXB)
  d[, tsa := pr(runs_pre + actual_runs, 10L - wkts_pre - is_wicket, pmax(0L, MAXB - bb)) -
             pr(runs_pre + exp_runs, 10 - wkts_pre - exp_wicket, pmax(0L, MAXB - bb))]

  # ANCHORS, fixed before looking: a dot and a wicket must both reduce the
  # projected score, a six must raise it, and a wicket must cost more than a
  # dot. These hold in a chase exactly as they do in a first innings; if they
  # ever stop holding, the projection is being fed the wrong state.
  m_dot <- d[actual_runs == 0 & is_wicket == 0, mean(tsa, na.rm = TRUE)]
  m_wkt <- d[is_wicket == 1, mean(tsa, na.rm = TRUE)]
  m_six <- d[actual_runs == 6, mean(tsa, na.rm = TRUE)]
  if (!(m_dot < 0 && m_wkt < 0 && m_six > 0 && m_wkt < m_dot)) {
    stop(sprintf("%s %s inn%d: anchors failed (dot %+.3f wicket %+.3f six %+.3f)",
                 FMT, GEN, INN, m_dot, m_wkt, m_six))
  }

  DBI::dbWriteTable(conn, "tsa_stage", d[, .(delivery_id, tsa)], overwrite = TRUE, temporary = TRUE)
  n <- DBI::dbExecute(conn, "
    UPDATE main.cricsheet_ball_raa AS r SET tsa = s.tsa
    FROM tsa_stage s WHERE s.delivery_id = r.delivery_id")
  total <- total + n
  cat(sprintf("  %-4s %-6s inn%d %11s balls  mean tsa %+.4f  dot %+.3f  wicket %+.3f  six %+.3f\n",
              FMT, GEN, INN, format(n, big.mark=","), mean(d$tsa, na.rm=TRUE),
              m_dot, m_wkt, m_six))
}
cat(sprintf("\ntotal rows given a TSA: %s\n", format(total, big.mark=",")))

cat("\n=== coverage: what share of each bucket has TSA? ===\n")
print(DBI::dbGetQuery(conn, "
  SELECT format, gender, COUNT(*) AS balls,
         SUM(CASE WHEN tsa IS NOT NULL THEN 1 ELSE 0 END) AS with_tsa,
         ROUND(100.0*SUM(CASE WHEN tsa IS NOT NULL THEN 1 ELSE 0 END)/COUNT(*),1) AS pct
  FROM main.cricsheet_ball_raa GROUP BY 1,2 ORDER BY 1,2"))
cat("  both innings now, so ~100% in limited overs and 0% in Test is expected\n")

cat("\n=== the chase intent bias, by how many runs are still needed ===\n")
cat("  If securing the target made batters block, the 'already won' band would\n")
cat("  show actual well below expected. It does not, and the band is tiny --\n")
cat("  the innings ends when the target is passed, so those rows barely exist.\n")
print(DBI::dbGetQuery(conn, "
  WITH tgt AS (
    SELECT match_id, MAX(total_runs) AS inn1_total
    FROM cricsheet.deliveries WHERE innings = 1 GROUP BY match_id)
  SELECT CASE WHEN t.inn1_total + 1 - (d.total_runs - d.runs_total) <= 0 THEN 'already won'
              WHEN t.inn1_total + 1 - (d.total_runs - d.runs_total) <= 10 THEN '1-10'
              WHEN t.inn1_total + 1 - (d.total_runs - d.runs_total) <= 30 THEN '11-30'
              WHEN t.inn1_total + 1 - (d.total_runs - d.runs_total) <= 60 THEN '31-60'
              ELSE '60+' END AS runs_needed,
         COUNT(*) AS n,
         ROUND(AVG(r.tsa), 4) AS mean_tsa,
         ROUND(AVG(r.actual_runs), 3) AS mean_actual,
         ROUND(AVG(r.exp_runs), 3) AS mean_expected
  FROM main.cricsheet_ball_raa r
  JOIN cricsheet.deliveries d ON d.delivery_id = r.delivery_id
  JOIN tgt t ON t.match_id = d.match_id
  WHERE r.innings_number = 2 AND r.tsa IS NOT NULL AND r.format = 'T20' AND r.gender = 'male'
  GROUP BY 1 ORDER BY n DESC"))
