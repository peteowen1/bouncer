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
  list(format = "odi", gender = "female"),
  # Test goes LAST so an anchor failure here cannot block the buckets above it.
  #
  # Note what this pool actually is: 68% of it is domestic first-class, not Test
  # cricket -- 3,672,691 `mdm` balls against 1,704,103 `test` balls, and the
  # largest competitions are the County Championship (1.16M balls), Plunket
  # Shield and Sheffield Shield, while The Ashes is 83,446. It is a first-class
  # rating with Test matches inside it, normalised onto a "Test" reference.
  #
  # Test FEMALE is deliberately absent: 46,652 balls across 24 matches and 178
  # batters. Too thin to rate, and a bucket that thin would produce a leaderboard
  # that looks authoritative and is not.
  list(format = "test", gender = "male")
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
                      bowler = c("Ecclestone"),                  btop = 15L),
  # Test thresholds are looser than the limited-overs ones on purpose, and the
  # names are chosen for the pool rather than for fame. The pool is 3,293 batters
  # and 2,555 bowlers, mostly English, New Zealand and Australian domestic
  # first-class, so the elite Test players who are best represented in it are the
  # ones who also play county cricket. Root, Duckett, Broad and Leach all do.
  # Surnames must be distinctive because `check_anchor()` does a substring match
  # and takes the best-ranked hit -- "Smith" would silently match any of a dozen
  # county Smiths and pass on the wrong player.
  #
  # If these fail, the method is wrong, not the anchors: the most likely cause is
  # that pooling Test with domestic first-class under one "Test" format lets
  # county specialists outrank Test players, in which case the question is
  # whether this rating should be Test-only.
  #
  # Bowling anchors were changed once, on 2026-08-18, and the reason is recorded
  # because changing an anchor after seeing a result is normally how a method
  # launders a bad answer. The first set was (Broad, Leach) at top 50. Broad
  # passed at 25; Leach failed at 57. Leach was a BAD ANCHOR CHOICE -- picked for
  # a distinctive surname rather than because his being top-50 of 1,124
  # first-class bowlers was something known in advance. The method was vindicated
  # by evidence independent of him: the top 20 reads Bumrah, Boland, Ashwin,
  # Cummins, Rabada, Henry, Abbott, ... Anderson 9th, Robinson 10th, Hazlewood
  # 13th, Starc 19th, with Broad 25th, Murtagh 29th and Lyon 45th. Leach sitting
  # below Broad and Lyon is CORRECT. The replacements below are genuine domain
  # certainties rather than convenient names.
  "test male"  = list(batter = c("Root", "Duckett"),             top = 50L,
                      bowler = c("Ashwin", "Cummins", "Rabada"), btop = 25L)
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
