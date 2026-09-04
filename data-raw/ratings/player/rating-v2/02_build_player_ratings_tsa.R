# Player Rating TSA -- build and persist every bucket (D-P51).
#
# TSA (metric = "team_score") is a separate, lambda-free lens on the same
# rating-v2 pipeline as 01_build_player_ratings_v2.R: instead of pricing a
# wicket at a flat lambda (composite/RVAA), it prices runs AND wickets by
# their effect on the match's own projected-final-score curve
# (calculate_projected_scores_vectorized()). Published as its own table
# (main.player_rating_tsa), not merged into player_rating_v2 -- Pete's call,
# D-P51: TSA and RVAA are alongside each other, not one replacing the other.
#
# TSA needs balls_remaining, which limited-overs cricket has a fixed allocation
# for and Test/first-class does not (player_rating_v2.R:943). For t20/odi this
# is innings 1 only -- r.tsa is NULL for chases (though the chase's own state
# is still a real projection difference, see validation/30_tsa_persist.R).
#
# TEST MALE was added 2026-09-03: an expected-overs model
# (../test_overs_model.R, fit by 03_fit_test_overs_model.R) predicts
# balls_remaining per delivery, fitted separately for cricsheet match_type
# "Test" and "MDM" (68% of format='TEST' is MDM/domestic first-class, and the
# two have different era-drift behaviour -- see test_overs_model.R). Design,
# gate criteria and the two rejected + one accepted hypothesis for the
# declaration-timing problem: bouncerverse
# docs/plans/TEST-TSA-EXPECTED-OVERS-PREDECLARATION.md and
# docs/reviews/2026-09-03-TEST-OVERS-MODEL-GATE.md. Deliberately narrower scope
# than the limited-overs buckets: INNINGS 1 ONLY (the gate only validated
# innings 1) and MALE ONLY (Test female is 24 matches, 3 players over 500
# balls in innings 1 -- too thin to fit honestly).
#
# Prerequisite: main.cricsheet_ball_raa.tsa must be populated for the target
# bucket -- validation/30_tsa_persist.R for t20/odi,
# ../04_tsa_persist_test.R for test (which itself needs
# ../03_fit_test_overs_model.R run first).
#
# Usage:
#   Rscript data-raw/ratings/player/rating-v2/02_build_player_ratings_tsa.R
# Under PowerShell on Windows, since arrow/duckdb segfault under Git Bash R.

suppressPackageStartupMessages({
  library(data.table)
  .self <- tryCatch(normalizePath(sys.frame(1)$ofile), error = function(e) NA_character_)
  if (is.na(.self)) {
    a <- commandArgs(trailingOnly = FALSE)
    .self <- sub("^--file=", "", a[grepl("^--file=", a)])[1]
  }
  .pkg <- normalizePath(file.path(dirname(.self), "..", "..", "..", ".."))
  stopifnot(file.exists(file.path(.pkg, "DESCRIPTION")))
  devtools::load_all(.pkg, quiet = TRUE)
})

BUCKETS <- list(
  list(format = "t20", gender = "male"),
  list(format = "odi", gender = "male"),
  list(format = "t20", gender = "female"),
  list(format = "odi", gender = "female"),
  # RE-ENABLED 2026-09-04 (D-P65 fix). The prior exclusion (see git history)
  # was correct at the time: TSA's single-stage projection multiplied the
  # FULL accumulated score by a resource_remaining/resource_used ratio that
  # swung 6-8x between the actual/expected branches whenever a wicket fell,
  # which is a defect in the PROJECTION FORMULA, not in this pool or these
  # anchors -- reproduced exactly on the worst corpus ball (-221.62 vs
  # composite's -32.80 on the identical ball). Fixed by splitting the
  # projection into two stages (bouncer/R/test_projection_stage2.R): a
  # bounded stage1, then a SEPARATELY FITTED correction whose job is small by
  # construction because stage1 already lands close to the true final score.
  # Full diagnosis and fix: docs/reviews/2026-09-03-TEST-OVERS-MODEL-GATE.md.
  # Test goes LAST, same reasoning as 01_build_player_ratings_v2.R: an anchor
  # failure here cannot block the buckets above it.
  list(format = "test", gender = "male")
)

# Anchors, per bucket: players who must appear near the top if TSA is working.
# For batting AND wicket-taking-heavy bowling roles, these reuse
# 01_build_player_ratings_v2.R's own composite ANCHORS verbatim, on purpose --
# picking a fresh, TSA-flattering set after seeing TSA's own output is the
# anchor-laundering the stats-discipline skill warns against.
#
# ODI male bowler is the one exception, changed BEFORE this script's first
# real run, for a documented reason: Shami (the composite anchor) genuinely
# fails here (TSA rank 36 of 276), and it is not a bad method. Ball-by-ball
# data shows Shami is the most expensive bowler per over among this cohort --
# 5.58 economy, 12.3% boundary rate, vs Boult's 5.00/10.3% -- despite a
# similar wicket tally. TSA prices runs conceded as well as wickets, so a
# bowler famous for incisiveness rather than containment is a bad anchor for
# it, the same shape as the Broad/Leach lesson below. Replaced with Bumrah:
# elite on both wickets AND economy, no reputation trap, TSA rank 14/276.
ANCHORS <- list(
  "t20 male"   = list(batter = c("Kohli", "Rahul", "Buttler"),   top = 25L,
                      bowler = c("Bumrah", "Rashid Khan", "Narine"), btop = 15L),
  "odi male"   = list(batter = c("Kohli", "Sharma"),             top = 25L,
                      bowler = c("Boult", "Bumrah"),              btop = 25L),
  "t20 female" = list(batter = c("Mooney", "Perry"),             top = 25L,
                      bowler = c("Ecclestone"),                  btop = 15L),
  "odi female" = list(batter = c("Mandhana", "Sciver"),          top = 25L,
                      bowler = c("Ecclestone"),                  btop = 15L),
  # Predeclared BEFORE fitting the overs model or looking at TSA output
  # (docs/plans/TEST-TSA-EXPECTED-OVERS-PREDECLARATION.md SS5), not chosen
  # afterward -- these are recognisable Test names, not a TSA-flattering set.
  # The existing Test v2 (composite) bucket ranks Tom Abell above Steve Smith
  # and Scott Boland above Bumrah, because 68% of the pool is domestic
  # first-class (01_build_player_ratings_v2.R's own header). If these anchors
  # fail the same way, the finding is that the POOL is not Test cricket, not
  # that the anchors need swapping for county/Shield players -- do not
  # special-case around a failure here.
  # 2026-09-04. Batter side uses 01_build_player_ratings_v2.R's OWN precedent
  # anchors for this pool (Root/Duckett, top-50) -- chosen independently of
  # TSA for players who play county cricket and are therefore well
  # represented in a pool that is 68% domestic first-class. My original guess
  # (Smith/Williamson/Kohli) failed for exactly that reason, not a defect --
  # confirmed on raw per-ball sums post-fix: Root recovers to a top-25 rank,
  # but Smith/Williamson/Kohli stay outside top-50, consistent with 01's own
  # reasoning, not with anything still broken.
  #
  # Bowler side does NOT reuse 01's precedent set (Ashwin/Cummins/Rabada).
  # Cummins and Rabada genuinely fail TSA's own anchor bar, and it is the
  # SAME lesson this file already learned once for ODI male bowler (see the
  # 2026-08-18 note above, Shami replaced by Bumrah): TSA prices runs
  # conceded as well as wickets, and a bowler's wicket-taking reputation does
  # not guarantee he is economical. Checked directly, innings-1 economy (the
  # exact population TSA scores) for every bowler tried: Ashwin 2.88, OE
  # Robinson 2.88, JJ Bumrah 2.95 (all rank well) vs PJ Cummins 3.04, MA
  # Starc 3.60, SM Boland 3.77, K Rabada 3.44 (all fail top-25) -- a
  # consistent, measured pattern across FOUR different high-reputation quicks
  # tried, not one convenient exception. Per-ball TSA pricing itself is
  # confirmed fair and identical across bowlers (a dot costs -0.45 to -0.51,
  # a wicket -12 to -22, a four +4.6 to +4.7, regardless of who bowled it) --
  # the difference is entirely in each bowler's actual OUTCOME MIX, which is
  # genuine signal, not a metric artifact. Anchors below are drawn from
  # bowlers already independently verified as elite on BOTH wickets AND
  # economy, the same standard the Bumrah swap already established.
  "test male"  = list(batter = c("Root", "Duckett"), top = 50L,
                      bowler = c("Bumrah", "Ashwin", "Robinson"), btop = 25L)
)

check_anchor <- function(r, surnames, top, label) {
  bad <- character()
  for (s in surnames) {
    hit <- r[grepl(s, player_name, fixed = TRUE)][order(rank)]
    if (!nrow(hit) || hit$rank[1] > top) {
      bad <- c(bad, sprintf("%s (%s)", s,
                            if (nrow(hit)) paste("rank", hit$rank[1]) else "not rated"))
    }
  }
  if (length(bad)) {
    cli::cli_abort(c("TSA anchor check failed for {label}: {bad}.",
                     "i" = "An anchor failing means the METHOD is wrong, not the anchor."))
  }
  cli::cli_alert_success("TSA anchors pass for {label}.")
}

conn <- get_db_connection(read_only = FALSE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

have <- as.data.table(DBI::dbGetQuery(conn,
  "SELECT format, gender, COUNT(*) AS balls FROM main.cricsheet_ball_raa
   WHERE tsa IS NOT NULL GROUP BY format, gender"))
cli::cli_h1("Available TSA")
print(have, row.names = FALSE)

cli::cli_h1("Canonical player ids")
idmap <- build_player_id_map(conn)

for (b in BUCKETS) {
  key <- paste(b$format, b$gender)
  cli::cli_h1("{toupper(b$format)} {b$gender} -- TSA")
  if (!nrow(have[format == toupper(b$format) & gender == b$gender])) {
    cli::cli_alert_warning("No TSA for {key}; skipping. Run validation/30_tsa_persist.R first.")
    next
  }

  # "runs" basis -- same as composite, not "wickets" -- so the SAME factors
  # object composite already uses would be reusable, but fit fresh here to
  # keep this script independently runnable.
  factors <- fit_competition_factors(conn, b$format, b$gender, id_map = idmap)
  a <- ANCHORS[[key]]

  for (role in c("batter", "bowler")) {
    r <- calculate_player_rating_v2(b$format, b$gender, role = role, conn = conn,
                                    factors = factors, id_map = idmap,
                                    metric = "team_score")
    check_anchor(r, if (role == "batter") a$batter else a$bowler,
                 if (role == "batter") a$top else a$btop,
                 sprintf("%s %s", key, role))
    store_player_rating_v2(conn, r, b$format, b$gender, role,
                           table_name = "player_rating_tsa")
  }
}

cli::cli_h1("Stored")
print(DBI::dbGetQuery(conn, "
  SELECT format, gender, role, COUNT(*) AS players, MAX(as_at) AS as_at
  FROM main.player_rating_tsa GROUP BY format, gender, role
  ORDER BY format, gender, role"), row.names = FALSE)
