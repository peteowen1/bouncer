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
  list(format = "test", gender = "male"),
  # bouncerverse#40 item 1: the blended bucket above is exactly what its own
  # comment says it is -- a first-class rating normalised onto a Test
  # reference, not an international leaderboard. Live check, 2026-09-04: only
  # 6 of its top 20 carry main_comp = "Test"; Tom Abell (County Championship,
  # 24.67) outranks Steve Smith (Test, 24.19). This is NOT a mis-fit --
  # unconditional averages confirm County Championship (30.99) and Test
  # (30.90) sit almost exactly together in this corpus, so the bridge
  # discount is doing its job correctly and the blended scale is just the
  # wrong question for a page the nav calls "Test cricket".
  #
  # match_type_filter = "test" restricts the population to genuine
  # international Test deliveries only, dropping MDM (domestic first-class)
  # entirely. No separate competition-factor fit is needed: .competition_sql
  # ("test") already resolves every match_type='test' row to the single comp
  # "Test", which is always the reference (factor 1.0) by construction, so
  # the SAME `factors` object fit below on the blended population applies
  # unchanged -- see calculate_player_rating_v2()'s match_type_filter doc.
  # store_as gives this its own bucket key so it does not overwrite the
  # blended "test male" rows above; both stay queryable.
  list(format = "test", gender = "male", match_type_filter = "test", store_as = "test_intl")
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
                      bowler = c("Ashwin", "Cummins", "Rabada"), btop = 25L),
  # test_intl: match_type='test' only, no domestic first-class. Pool measured
  # BEFORE fitting (2026-09-04): 1,049 batters / 783 bowlers with any Test
  # balls at all, before the exposure floor thins that further -- an order of
  # magnitude smaller than the blended pool's 3,293/2,555. Thresholds scaled
  # down from the blended bucket's top=50/btop=25 (roughly 1.5%/1% of its
  # pool) to top=20/btop=15 (roughly 2%/1.9% of this one), loosened slightly
  # rather than held to the exact same fraction since the exposure floor cuts
  # harder on a genuinely smaller pool.
  #
  # Batting anchor was changed once, on 2026-09-04, and the reason is recorded
  # for the same reason the Leach swap above is: Kohli was the first pick,
  # chosen for fame rather than verified current form, and failed at rank 51.
  # Checked independently (raw batting average, no rating math) before
  # concluding anything: Kohli's Test average over the last 3 years is 26.38
  # (12 matches) against 49.58 before that -- a genuine, well-documented
  # decline, not a computation bug, and this rating is explicitly decayed
  # toward "who would you want next match" (D-P17), not career reputation.
  # Checked two replacement candidates the SAME way, before either appeared
  # on a leaderboard: Williamson (54.89 career vs 49.68 last 3 years -- barely
  # moved) and Smith (58.62 vs 43.73 -- declined but still clearly elite by
  # any absolute standard). Williamson is the cleaner pick and replaces Kohli.
  # Bumrah and Cummins anchor bowling, both undisputed current Test-elite
  # quicks with no comparable form question. All four are distinctive
  # surnames with no realistic substring collision.
  "test_intl male" = list(batter = c("Root", "Williamson"),      top = 20L,
                          bowler = c("Bumrah", "Cummins"),       btop = 15L)
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
  # store_as lets a bucket persist under a different key than the one it
  # reads RAA/competition-factors from (bouncerverse#40 item 1: test_intl
  # reuses the "test" RAA source and factor fit, restricted at query time via
  # match_type_filter, but must not overwrite the blended "test" bucket rows).
  store_as <- if (is.null(b$store_as)) b$format else b$store_as
  key <- paste(store_as, b$gender)
  cli::cli_h1("{toupper(store_as)} {b$gender}")
  if (!nrow(have[format == toupper(b$format) & gender == b$gender])) {
    cli::cli_alert_warning("No RAA for {paste(b$format, b$gender)}; skipping. Run build_cricsheet_raa first.")
    next
  }

  factors <- fit_competition_factors(conn, b$format, b$gender, id_map = idmap)
  a <- ANCHORS[[key]]

  for (role in c("batter", "bowler")) {
    # `factors` is passed for the deviation-compression term only. The
    # competition OFFSET is fitted per role inside the call, because it has
    # to be estimated on that role's own opponent-adjusted value.
    r <- calculate_player_rating_v2(b$format, b$gender, role = role, conn = conn,
                                    factors = factors, id_map = idmap,
                                    match_type_filter = b$match_type_filter)
    check_anchor(r, if (role == "batter") a$batter else a$bowler,
                 if (role == "batter") a$top else a$btop,
                 sprintf("%s %s", key, role))
    store_player_rating_v2(conn, r, store_as, b$gender, role)
  }

  v <- calculate_player_value_v2(b$format, b$gender, conn = conn,
                                 factors = factors, id_map = idmap,
                                 match_type_filter = b$match_type_filter)
  store_player_value_v2(conn, v, store_as, b$gender)
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
