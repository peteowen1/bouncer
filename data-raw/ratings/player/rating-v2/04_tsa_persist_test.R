# Persist TSA into main.cricsheet_ball_raa for format='TEST' (bouncerverse D-P51
# follow-on, #84-adjacent design work).
#
# Mirrors validation/30_tsa_persist.R's limited-overs approach exactly, with one
# substitution: TSA needs balls_remaining, and Test/first-class have no fixed
# ball allocation to compute it from. That value comes from the fitted
# expected-overs model (test_overs_model.R, fit by
# 03_fit_test_overs_model.R) instead of a constant MAXB. Both terms of the TSA
# difference (actual vs expected outcome) use the SAME predicted
# balls_remaining, matching 30's formula shape exactly:
#
#   tsa = pr(actual outcome, br) - pr(expected outcome, br)
#
# UPDATED 2026-09-04 (D-P65): `pr()` is now the TWO-STAGE projection
# (test_projection_stage2.R, fit by 05_fit_test_stage2_correction.R), not the
# original calculate_projected_scores_vectorized(). The single-stage version
# multiplied the FULL accumulated score by a resource_remaining/resource_used
# ratio that swung 6-8x between the actual/expected branches whenever a
# wicket fell -- reproduced exactly on the worst corpus ball (-221.62, vs
# composite's -32.80 on the identical ball). The two-stage version fixes this:
# stage1 is bounded and monotonic (no ratio), and stage2 is a SEPARATE fitted
# correction, small by construction because stage1 already lands close to a
# sane final total. Full diagnosis and fix:
# bouncerverse docs/reviews/2026-09-03-TEST-OVERS-MODEL-GATE.md.
#
# INNINGS 1 ONLY, unlike 30's "both innings" choice for limited overs. This is a
# deliberate, narrower scope than 30's precedent, not an oversight: the gate
# (docs/reviews/2026-09-03-TEST-OVERS-MODEL-GATE.md) validated the overs model
# and the resulting TSA rating's rank agreement against an oracle ONLY on
# innings 1. The overs model itself was fit across all 4 innings (innings is a
# formula term), so extending to innings 2-4 later is possible, but it needs its
# own anchor/rank-agreement check first -- not silently inherited from this
# gate. Also MALE ONLY: Test female is 24 matches with 3 players over 500 balls
# in innings 1, too thin for an honest fit (predeclaration, scope decisions).
#
# `format='TEST'` in cricsheet_ball_raa spans TWO cricsheet match_types, Test
# and MDM (domestic first-class) -- 68% of the rows are MDM. The overs model
# AND the stage-2 correction are fitted and applied SEPARATELY per match_type
# (see test_overs_model.R for why: Test innings length collapsed 2021-2024,
# MDM shows no era drift over the same span), so this script processes them
# as two passes with two different loaded model pairs, writing to the same
# table.
#
# Usage: Rscript data-raw/ratings/player/rating-v2/04_tsa_persist_test.R
# Under PowerShell on Windows, since arrow/duckdb segfault under Git Bash R.
# Requires 03_fit_test_overs_model.R and 05_fit_test_stage2_correction.R to
# have been run first.

suppressPackageStartupMessages({
  library(DBI); library(data.table)
  devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE)
})
DB <- file.path(find_bouncerdata_dir(), "bouncer.duckdb")
stopifnot(file.exists(DB), file.info(DB)$size > 1e10)
conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = DB, read_only = FALSE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

have <- DBI::dbGetQuery(conn, "
  SELECT column_name FROM information_schema.columns
  WHERE table_schema='main' AND table_name='cricsheet_ball_raa'")$column_name
stopifnot("tsa" %in% have)  # created by validation/30_tsa_persist.R; must run first

EIS <- list(Test = EIS_TEST_MALE_INTL, MDM = EIS_TEST_MALE_CLUB)
total <- 0L

for (MT in c("Test", "MDM")) {
  overs_model <- load_test_overs_model(MT)
  stage2 <- load_test_stage2_correction(MT)

  d <- as.data.table(DBI::dbGetQuery(conn, sprintf("
    SELECT r.delivery_id, r.match_id, r.innings_number AS innings, r.actual_runs,
           CAST(r.is_wicket AS INT) AS is_wicket, r.exp_runs, r.exp_wicket,
           r.over_number, r.ball_number,
           d.total_runs - d.runs_total AS runs_pre,
           d.wickets_fallen - CAST(d.is_wicket AS INT) AS wkts_pre
    FROM main.cricsheet_ball_raa r
    JOIN cricsheet.deliveries d ON d.delivery_id = r.delivery_id
    JOIN cricsheet.matches m    ON m.match_id    = r.match_id
    WHERE r.format = 'TEST' AND r.gender = 'male' AND r.innings_number = 1
      AND m.match_type = '%s'", MT)))

  if (!nrow(d)) { cat(sprintf("  %s: no rows\n", MT)); next }

  # Ball position comes from over_number/ball_number, NOT a ROW_NUMBER() over
  # this join -- main.cricsheet_ball_raa is missing a handful of balls per
  # innings (its own upstream RAA pipeline drops some deliveries -- measured
  # 7-9 missing balls scattered through several hundred Test innings), so
  # row-counting the joined result mis-numbers every ball after a gap. 10 of
  # 886 Test innings are missing their OPENING ball specifically, which a
  # ROW_NUMBER-based ball index cannot detect -- it just starts counting from
  # the next ball as if it were the first, silently understating balls_before
  # by however many were dropped before it. over_number/ball_number are the
  # delivery's own labelled position and are unaffected by which OTHER rows
  # the join happens to be missing. balls_per_over is always 6 for Test/MDM
  # (verified: a single distinct value across every match of both types).
  d[, balls_before := over_number * 6L + ball_number - 1L]
  innings_end <- d[, .(innings_balls = max(balls_before) + 1L), by = match_id]
  d <- merge(d, innings_end, by = "match_id", all.x = TRUE)

  # FRAME CHECK, same discipline as 30_tsa_persist.R: verify the opening ball of
  # every innings has runs_pre == 0, i.e. the score restarts each innings rather
  # than being cumulative across the match. Checked on balls_before == 0 (the
  # delivery's own labelled position), which survives a missing-row gap
  # elsewhere in the innings -- a ROW_NUMBER-based version of this same check
  # does NOT survive that gap, and caught its own bug by failing here first
  # (10 matches, opener runs_pre up to 5) before this fix was made.
  opener <- d[balls_before == 0L]
  if (!nrow(opener)) stop(sprintf("%s: no opening ball found, frame unverified", MT))
  if (max(opener$runs_pre) > 0) {
    stop(sprintf("%s: score does not restart (max runs_pre %s on the opening ball)",
                 MT, max(opener$runs_pre)))
  }

  # innings==1 always here, so lead is always 0 and match_balls_before ==
  # balls_before -- both are still passed through so the model sees the exact
  # feature shape it was fit on.
  d[, lead := 0]
  d[, run_rate := ifelse(balls_before > 0, runs_pre / balls_before, 0)]
  d[, match_balls_before := balls_before]
  d[, wkt := factor(pmin(9L, wkts_pre), levels = 0:9)]
  d[, inn := factor(1L, levels = 1:4)]

  # Two-stage projection (D-P65). Both branches of the TSA difference share
  # the SAME match_type, so a scalar-per-call is fine here (unlike the
  # general-purpose calculate_test_projected_scores_v2(), which vectorises
  # across mixed match_type inputs).
  om_list <- setNames(list(overs_model), MT)
  s2_list <- setNames(list(stage2), MT)
  d[, tsa := calculate_test_projected_scores_v2(
    match_type = rep(MT, .N), current_score = runs_pre + actual_runs,
    wickets_before = wkts_pre, wickets_remaining = 10L - wkts_pre - is_wicket,
    balls_before = balls_before, run_rate = run_rate,
    lead = lead, innings = rep(1L, .N), match_balls_before = match_balls_before,
    eis = rep(EIS[[MT]], .N), overs_models = om_list, stage2_corrections = s2_list) -
    calculate_test_projected_scores_v2(
    match_type = rep(MT, .N), current_score = runs_pre + exp_runs,
    wickets_before = wkts_pre, wickets_remaining = 10 - wkts_pre - exp_wicket,
    balls_before = balls_before, run_rate = run_rate,
    lead = lead, innings = rep(1L, .N), match_balls_before = match_balls_before,
    eis = rep(EIS[[MT]], .N), overs_models = om_list, stage2_corrections = s2_list)]

  # ANCHORS, fixed before looking, matching 30_tsa_persist.R's own check exactly
  # so the same bar applies to every bucket that ever gets a tsa column: a dot
  # and a wicket must both reduce the projected score, a six must raise it, and
  # a wicket must cost more than a dot.
  m_dot <- d[actual_runs == 0 & is_wicket == 0, mean(tsa, na.rm = TRUE)]
  m_wkt <- d[is_wicket == 1, mean(tsa, na.rm = TRUE)]
  m_six <- d[actual_runs == 6, mean(tsa, na.rm = TRUE)]
  if (!(m_dot < 0 && m_wkt < 0 && m_six > 0 && m_wkt < m_dot)) {
    stop(sprintf("%s: anchors failed (dot %+.3f wicket %+.3f six %+.3f)",
                 MT, m_dot, m_wkt, m_six))
  }

  DBI::dbWriteTable(conn, "tsa_test_stage", d[, .(delivery_id, tsa)], overwrite = TRUE, temporary = TRUE)
  n <- DBI::dbExecute(conn, "
    UPDATE main.cricsheet_ball_raa AS r SET tsa = s.tsa
    FROM tsa_test_stage s WHERE s.delivery_id = r.delivery_id")
  total <- total + n
  cat(sprintf("  %-4s inn1 %11s balls  mean tsa %+.4f  dot %+.3f  wicket %+.3f  six %+.3f\n",
              MT, format(n, big.mark = ","), mean(d$tsa, na.rm = TRUE), m_dot, m_wkt, m_six))
}
cat(sprintf("\ntotal rows given a TSA: %s\n", format(total, big.mark = ",")))

cat("\n=== coverage: what share of format='TEST' has TSA? ===\n")
print(DBI::dbGetQuery(conn, "
  SELECT format, gender, COUNT(*) AS balls,
         SUM(CASE WHEN tsa IS NOT NULL THEN 1 ELSE 0 END) AS with_tsa,
         ROUND(100.0*SUM(CASE WHEN tsa IS NOT NULL THEN 1 ELSE 0 END)/COUNT(*),1) AS pct
  FROM main.cricsheet_ball_raa WHERE format='TEST' GROUP BY 1,2 ORDER BY 1,2"))
cat("  innings 1 only + male only, so this bucket tops out well under 100% -- expected.\n")

cat("\n=== outlier check (D-P65's own lesson: always look) ===\n")
print(DBI::dbGetQuery(conn, "
  SELECT ROUND(MIN(tsa),2) mn, ROUND(QUANTILE_CONT(tsa,0.01),2) p01,
         ROUND(QUANTILE_CONT(tsa,0.5),3) p50, ROUND(QUANTILE_CONT(tsa,0.99),2) p99,
         ROUND(MAX(tsa),2) mx, ROUND(AVG(tsa),4) mean, ROUND(STDDEV(tsa),3) sd
  FROM main.cricsheet_ball_raa WHERE format='TEST' AND gender='male' AND tsa IS NOT NULL"))
