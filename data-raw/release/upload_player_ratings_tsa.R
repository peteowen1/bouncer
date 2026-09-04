#!/usr/bin/env Rscript
# Publish the TSA player ratings (D-P51) so downstream consumers do not need
# the 18GB database. Mirrors upload_player_ratings_v2.R exactly -- same
# validation shape, separate release tag, because TSA is a separate rating
# alongside RVAA (Pete's call), not a replacement column on the same table.
#
# One table, one file: player_rating_tsa.parquet, 8 buckets (t20/odi x
# male/female x batter/bowler -- no Test, TSA is structurally limited-overs
# only). No value-table analogue: TSA has no bat+bowl combined value yet.
#
# VALIDATES BEFORE UPLOADING -- same reasoning as the v2 script: publishing
# is outward-facing, so these are assertions, not warnings.
#
# Prerequisite: data-raw/ratings/player/rating-v2/02_build_player_ratings_tsa.R
# Usage (PowerShell; arrow segfaults under Git Bash R):
#   Rscript data-raw/release/upload_player_ratings_tsa.R

suppressPackageStartupMessages({
  library(DBI); library(arrow); library(piggyback); library(cli)
  library(data.table)
  devtools::load_all(here::here(), quiet = TRUE)
})

REPO <- "peteowen1/bouncerdata"
TAG  <- "player-rating-tsa"
DRY  <- isTRUE(as.logical(Sys.getenv("DRY_RUN", "false")))

token <- Sys.getenv("GITHUB_PAT", "")
if (!nzchar(token)) {
  token <- tryCatch(system2("gh", c("auth", "token"), stdout = TRUE)[1],
                    error = function(e) "")
}
if (!nzchar(token) && !DRY) cli::cli_abort("No GITHUB_PAT and `gh auth token` failed.")

cli::cli_h1("Player Rating TSA -> {REPO}@{TAG}{if (DRY) ' (DRY RUN)' else ''}")

conn <- get_db_connection(read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

ratings <- as.data.table(DBI::dbGetQuery(conn, "SELECT * FROM main.player_rating_tsa"))

# ---- validation -----------------------------------------------------------
cli::cli_h2("Validation")

check <- function(ok, msg) {
  if (!ok) cli::cli_abort("FAILED: {msg}")
  cli::cli_alert_success(msg)
}

check(nrow(ratings) > 0, "table is non-empty")

dup_r <- nrow(ratings) - nrow(unique(ratings, by = c("format", "gender", "role", "player_id")))
check(dup_r == 0, sprintf("no duplicate player per bucket (%d rows)", nrow(ratings)))

check(!anyNA(ratings$rating), "no NA ratings")
check(all(ratings[, .(ok = min(rank) == 1L & max(rank) == .N), by = .(format, gender, role)]$ok),
      "rank is 1..N within every bucket")
check(uniqueN(ratings[, .(format, gender, role)]) == 8L, "all 8 buckets present (no Test)")
check(!any(ratings$format == "TEST"), "no Test rows -- TSA has no fixed ball allocation to project against")

# same reasoning as upload_player_ratings_v2.R: re-check on what is about to
# be published, not on what was computed. ODI male bowler intentionally
# checks Bumrah, not Shami -- see 02_build_player_ratings_tsa.R's ANCHORS
# comment for why Shami is a bad anchor for a runs-conceded-aware metric.
anchor <- function(fmt, gen, which_role, surname, top) {
  r <- ratings[format == toupper(fmt) & gender == gen & role == which_role]
  hit <- r[grepl(surname, player_name, fixed = TRUE)][order(rank)]
  ok <- nrow(hit) > 0 && hit$rank[1] <= top
  if (!ok) cli::cli_abort("Anchor failed: {surname} in {fmt}/{gen}/{which_role} is {if (nrow(hit)) hit$rank[1] else 'unrated'}, needed <= {top}")
  cli::cli_alert_success("{fmt}/{gen}/{which_role}: {hit$player_name[1]} rank {hit$rank[1]}")
}
anchor("t20", "male",   "batter", "Kohli", 25L)
anchor("t20", "male",   "bowler", "Bumrah", 15L)
anchor("odi", "male",   "bowler", "Bumrah", 25L)
anchor("t20", "female", "bowler", "Ecclestone", 15L)
anchor("odi", "female", "batter", "Mandhana", 25L)

cli::cli_h2("Contents")
print(ratings[, .(players = .N, as_at = max(as_at)), by = .(format, gender, role)][
  order(format, gender, role)], row.names = FALSE)

# ---- write and upload ------------------------------------------------------
dir <- tempdir()
f1 <- file.path(dir, "player_rating_tsa.parquet")
arrow::write_parquet(ratings, f1)

rb <- as.data.table(arrow::read_parquet(f1))
check(nrow(rb) == nrow(ratings) && identical(names(rb), names(ratings)),
      sprintf("parquet round-trips (%d rows, %.0f KB)", nrow(rb), file.size(f1)/1024))

if (DRY) {
  cli::cli_alert_info("DRY_RUN set; wrote {f1}, uploading nothing.")
  quit(save = "no")
}

cli::cli_h2("Uploading")
tryCatch(
  piggyback::pb_release_create(repo = REPO, tag = TAG, .token = token,
                               name = "Player Ratings TSA"),
  error = function(e) cli::cli_alert_info("Release {TAG} already exists."))

try(memoise::forget(piggyback:::pb_releases), silent = TRUE)
Sys.sleep(3)
rel <- piggyback::pb_releases(repo = REPO, .token = token)
if (!TAG %in% rel$tag_name) {
  cli::cli_abort("Release {TAG} still not visible after refresh; not uploading.")
}

piggyback::pb_upload(f1, repo = REPO, tag = TAG, .token = token, overwrite = TRUE)
cli::cli_alert_success("Uploaded {basename(f1)}")

cli::cli_h2("Published assets")
cat(system2("gh", c("api", sprintf("repos/%s/releases/tags/%s", REPO, TAG),
                    "--jq", shQuote('.assets[] | "\\(.name)  \\(.size) bytes  updated \\(.updated_at)"')),
            stdout = TRUE), sep = "\n")
