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
# TSA only exists for innings 1 of limited-overs cricket -- r.tsa is NULL for
# chases (though the chase's own state is still a real projection difference,
# see 30_tsa_persist.R) and for Test, which has no fixed ball allocation
# (player_rating_v2.R:943). So this is t20/odi male/female only -- no Test
# bucket, structurally, not by omission.
#
# Prerequisite: main.cricsheet_ball_raa.tsa must be populated
# (data-raw/ratings/player/rating-v2/validation/30_tsa_persist.R).
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
  list(format = "odi", gender = "female")
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
                      bowler = c("Ecclestone"),                  btop = 15L)
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
