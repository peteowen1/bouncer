# Player Rating v2 -- build and persist every bucket.
#
# Runs the full chain for men's/women's T20 and ODI:
#
#   canonical player ids (#43)
#     -> competition difficulty factors, anchored per bucket
#     -> two-way batter/bowler opponent adjustment
#     -> batting and bowling ratings, per role
#     -> combined per-match-played value
#
# and writes the results to `main.player_rating_v2` and `main.player_value_v2`
# so downstream consumers do not need the 18GB database.
#
# Prerequisite: `main.cricsheet_ball_raa` must be populated for each bucket
# (build_cricsheet_raa). This script does not build it -- that is a separate,
# much heavier step, and silently rebuilding it here would hide a stale input.
#
# Usage:
#   Rscript data-raw/ratings/player/rating-v2/01_build_player_ratings_v2.R
# Under PowerShell on Windows, since arrow/duckdb segfault under Git Bash R.

suppressPackageStartupMessages({
  library(data.table)
  # here::here() walks up to the nearest project root, which from the verse
  # directory is bouncerverse and NOT the package -- the run then dies on a
  # missing DESCRIPTION. Resolve the package root from this script's own path
  # so it works from any working directory.
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

# Anchors, per bucket: players who must appear near the top if the pipeline is
# working. Chosen from domain knowledge, and asserted rather than eyeballed --
# a rating that silently degrades looks exactly like a rating that did not.
# Matched on surname, because registry spellings vary ("S Ecclestone" vs
# "SF Ecclestone") and a name lookup that misses returns nothing at all.
ANCHORS <- list(
  "t20 male"   = list(batter = c("Kohli", "Rahul", "Buttler"),   top = 25L,
                      bowler = c("Bumrah", "Rashid Khan", "Narine"), btop = 15L),
  "odi male"   = list(batter = c("Kohli", "Sharma"),             top = 25L,
                      bowler = c("Boult", "Shami"),              btop = 25L),
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
    cli::cli_abort(c("Anchor check failed for {label}: {bad}.",
                     "i" = "An anchor failing means the METHOD is wrong, not the anchor."))
  }
  cli::cli_alert_success("Anchors pass for {label}.")
}

conn <- get_db_connection(read_only = FALSE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

have <- as.data.table(DBI::dbGetQuery(conn,
  "SELECT format, gender, COUNT(*) AS balls FROM main.cricsheet_ball_raa
   GROUP BY format, gender"))
cli::cli_h1("Available RAA")
print(have, row.names = FALSE)

# built once and shared: it is the same registry for every bucket
cli::cli_h1("Canonical player ids")
idmap <- build_player_id_map(conn)

for (b in BUCKETS) {
  key <- paste(b$format, b$gender)
  cli::cli_h1("{toupper(b$format)} {b$gender}")
  if (!nrow(have[format == toupper(b$format) & gender == b$gender])) {
    cli::cli_alert_warning("No RAA for {key}; skipping. Run build_cricsheet_raa first.")
    next
  }

  factors <- fit_competition_factors(conn, b$format, b$gender, id_map = idmap)
  a <- ANCHORS[[key]]

  for (role in c("batter", "bowler")) {
    r <- calculate_player_rating_v2(b$format, b$gender, role = role, conn = conn,
                                    factors = factors, id_map = idmap)
    check_anchor(r, if (role == "batter") a$batter else a$bowler,
                 if (role == "batter") a$top else a$btop,
                 sprintf("%s %s", key, role))
    store_player_rating_v2(conn, r, b$format, b$gender, role)
  }

  v <- calculate_player_value_v2(b$format, b$gender, conn = conn,
                                 factors = factors, id_map = idmap)
  store_player_value_v2(conn, v, b$format, b$gender)
}

cli::cli_h1("Stored")
print(DBI::dbGetQuery(conn, "
  SELECT format, gender, role, COUNT(*) AS players, MAX(as_at) AS as_at
  FROM main.player_rating_v2 GROUP BY format, gender, role
  ORDER BY format, gender, role"), row.names = FALSE)
print(DBI::dbGetQuery(conn, "
  SELECT format, gender, COUNT(*) AS players, MAX(as_at) AS as_at
  FROM main.player_value_v2 GROUP BY format, gender ORDER BY format, gender"),
  row.names = FALSE)
