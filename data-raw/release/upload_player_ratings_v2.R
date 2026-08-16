#!/usr/bin/env Rscript
# Publish the v2 player ratings so downstream consumers do not need the 18GB
# database: inthegame-blog and the predictions pipeline both read from release
# parquets, not from DuckDB.
#
# Two tables, two files:
#   player_rating_v2.parquet   per-role ratings, 8 buckets
#   player_value_v2.parquet    combined batting+bowling value, 4 buckets
#
# VALIDATES BEFORE UPLOADING. Publishing is outward-facing and a release is
# what everything downstream trusts, so the checks are assertions and not
# warnings. Two of them exist because of specific incidents:
#   - COUNT(*) vs COUNT(DISTINCT): release parquets in this repo were found
#     ~2x duplicated on 2026-08-16 (bouncerdata#63), and a NOT IN guard does
#     not protect against duplicates INSIDE the source.
#   - anchor checks: a rating that silently degrades looks exactly like a
#     rating that did not.
#
# Prerequisite: data-raw/ratings/player/rating-v2/01_build_player_ratings_v2.R
# Usage (PowerShell; arrow segfaults under Git Bash R):
#   Rscript data-raw/release/upload_player_ratings_v2.R

suppressPackageStartupMessages({
  library(DBI); library(arrow); library(piggyback); library(cli)
  library(data.table)
  devtools::load_all(here::here(), quiet = TRUE)
})

REPO <- "peteowen1/bouncerdata"
TAG  <- "player-rating-v2"
DRY  <- isTRUE(as.logical(Sys.getenv("DRY_RUN", "false")))

token <- Sys.getenv("GITHUB_PAT", "")
if (!nzchar(token)) {
  token <- tryCatch(system2("gh", c("auth", "token"), stdout = TRUE)[1],
                    error = function(e) "")
}
if (!nzchar(token) && !DRY) cli::cli_abort("No GITHUB_PAT and `gh auth token` failed.")

cli::cli_h1("Player Rating v2 -> {REPO}@{TAG}{if (DRY) ' (DRY RUN)' else ''}")

conn <- get_db_connection(read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

ratings <- as.data.table(DBI::dbGetQuery(conn, "SELECT * FROM main.player_rating_v2"))
values  <- as.data.table(DBI::dbGetQuery(conn, "SELECT * FROM main.player_value_v2"))

# ---- validation -----------------------------------------------------------
cli::cli_h2("Validation")

check <- function(ok, msg) {
  if (!ok) cli::cli_abort("FAILED: {msg}")
  cli::cli_alert_success(msg)
}

check(nrow(ratings) > 0 && nrow(values) > 0, "both tables are non-empty")

# duplicates, the trap that cost an hour on 2026-08-16
dup_r <- nrow(ratings) - nrow(unique(ratings, by = c("format", "gender", "role", "player_id")))
dup_v <- nrow(values)  - nrow(unique(values,  by = c("format", "gender", "player_id")))
check(dup_r == 0, sprintf("no duplicate player per bucket in ratings (%d rows)", nrow(ratings)))
check(dup_v == 0, sprintf("no duplicate player per bucket in values (%d rows)", nrow(values)))

check(!anyNA(ratings$rating) && !anyNA(values$total_value), "no NA ratings or values")
check(all(ratings[, .(ok = min(rank) == 1L & max(rank) == .N), by = .(format, gender, role)]$ok),
      "rank is 1..N within every rating bucket")
check(all(values[, .(ok = min(rank) == 1L & max(rank) == .N), by = .(format, gender)]$ok),
      "rank is 1..N within every value bucket")
check(uniqueN(ratings[, .(format, gender, role)]) == 8L, "all 8 rating buckets present")
check(uniqueN(values[, .(format, gender)]) == 4L, "all 4 value buckets present")
check(all(values$total_value == round(values$bat_value + values$bowl_value, 10) |
          abs(values$total_value - (values$bat_value + values$bowl_value)) < 1e-8),
      "total_value equals bat_value + bowl_value")

# anchors: the same ones the build script asserts, re-checked on what is about
# to be published rather than on what was computed
# `which_role`, not `role`: a parameter named after a column is shadowed by that
# column inside `[...]`, so `role == role` silently matches every row instead of
# filtering. Renaming the parameter is the fix.
anchor <- function(fmt, gen, which_role, surname, top) {
  r <- ratings[format == toupper(fmt) & gender == gen & role == which_role]
  hit <- r[grepl(surname, player_name, fixed = TRUE)][order(rank)]
  ok <- nrow(hit) > 0 && hit$rank[1] <= top
  if (!ok) cli::cli_abort("Anchor failed: {surname} in {fmt}/{gen}/{which_role} is {if (nrow(hit)) hit$rank[1] else 'unrated'}, needed <= {top}")
  cli::cli_alert_success("{fmt}/{gen}/{which_role}: {hit$player_name[1]} rank {hit$rank[1]}")
}
anchor("t20", "male",   "batter", "Kohli", 25L)
anchor("t20", "male",   "bowler", "Bumrah", 15L)
anchor("odi", "male",   "batter", "Kohli", 25L)
anchor("t20", "female", "bowler", "Ecclestone", 15L)
anchor("odi", "female", "batter", "Mandhana", 15L)

cli::cli_h2("Contents")
print(ratings[, .(players = .N, as_at = max(as_at)), by = .(format, gender, role)][
  order(format, gender, role)], row.names = FALSE)
print(values[, .(players = .N, as_at = max(as_at)), by = .(format, gender)][
  order(format, gender)], row.names = FALSE)

# ---- write and upload ------------------------------------------------------
dir <- tempdir()
f1 <- file.path(dir, "player_rating_v2.parquet")
f2 <- file.path(dir, "player_value_v2.parquet")
arrow::write_parquet(ratings, f1)
arrow::write_parquet(values,  f2)

# read back what will actually be published, not what is in memory
rb <- as.data.table(arrow::read_parquet(f1))
check(nrow(rb) == nrow(ratings) && identical(names(rb), names(ratings)),
      sprintf("parquet round-trips (%d rows, %.0f KB)", nrow(rb), file.size(f1)/1024))

if (DRY) {
  cli::cli_alert_info("DRY_RUN set; wrote {f1} and {f2}, uploading nothing.")
  quit(save = "no")
}

cli::cli_h2("Uploading")
tryCatch(
  piggyback::pb_release_create(repo = REPO, tag = TAG, .token = token,
                               name = "Player Ratings v2"),
  error = function(e) cli::cli_alert_info("Release {TAG} already exists."))

# piggyback memoises the release list, so a release it just created is not in
# its own cache and the next pb_upload() reports "Release not found". Clearing
# the memo makes the create-then-upload sequence work on a first run.
try(memoise::forget(piggyback:::pb_releases), silent = TRUE)
Sys.sleep(3)
rel <- piggyback::pb_releases(repo = REPO, .token = token)
if (!TAG %in% rel$tag_name) {
  cli::cli_abort("Release {TAG} still not visible after refresh; not uploading.")
}

for (f in c(f1, f2)) {
  piggyback::pb_upload(f, repo = REPO, tag = TAG, .token = token, overwrite = TRUE)
  cli::cli_alert_success("Uploaded {basename(f)}")
}

# Release createdAt reflects the TAG, not the assets -- always report
# asset-level updatedAt (the gotcha in the root CLAUDE.md). Read straight from
# the API rather than piggyback::pb_list(), which serves its own memoised view
# and returned NULL immediately after an upload.
cli::cli_h2("Published assets")
cat(system2("gh", c("api", sprintf("repos/%s/releases/tags/%s", REPO, TAG),
                    "--jq", shQuote('.assets[] | "\\(.name)  \\(.size) bytes  updated \\(.updated_at)"')),
            stdout = TRUE), sep = "\n")
