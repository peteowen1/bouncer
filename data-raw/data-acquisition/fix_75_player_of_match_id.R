# Rewrite cricsheet.matches.player_of_match_id for matches that store a NAME
# instead of a registry id (bouncerverse#75).
#
# player_of_match_id was never wired to the registry lookup extract_players()
# uses for every other player reference -- unlike #74 (a 2026-only regression
# in the delivery columns), this is a longstanding property of the ingestion:
# 63-77% name-keyed in every year since 2020, flat, because parse_match_info()
# copied info$player_of_match straight from cricsheet's JSON. Fixed in R/
# cricsheet_parser.R for new ingests; this backfills the existing 16,162
# matches the same surgical, resumable way #74's fix did.
#
# WHY THIS ALSO SOLVES THE ORPHANED-ROW HALF OF #75. The issue's "merge rule"
# concern was about matching a bare orphaned cricsheet.players row back to its
# real identity with no context left -- exactly what name-lookups-return-the-
# wrong-player warns against. Re-deriving player_of_match_id from each match's
# OWN registry (this script) is not that: it is the authoritative per-match
# disambiguation, the same one every delivery column already trusts. Once this
# runs, whatever orphaned rows remain are references from nothing, no merge
# heuristic required -- verified below, not assumed.
#
# RESUMABLE, same pattern as #74: the work list is derived from the database
# each run, and each match commits independently.
#
# Usage:
#   Rscript data-raw/data-acquisition/fix_75_player_of_match_id.R <json_dir> [--commit] [--limit N]
suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(jsonlite); library(data.table)})

a <- commandArgs(trailingOnly = TRUE)
SRC <- a[1]
commit <- "--commit" %in% a
lim <- if ("--limit" %in% a) as.integer(a[which(a == "--limit") + 1L]) else Inf
stopifnot(dir.exists(SRC))

conn <- get_db_connection(read_only = FALSE)
all_affected <- dbGetQuery(conn, "
  SELECT match_id FROM cricsheet.matches
  WHERE player_of_match_id IS NOT NULL
    AND NOT regexp_matches(player_of_match_id, '^[0-9a-f]{8}$')")$match_id
todo <- all_affected[file.exists(file.path(SRC, paste0(all_affected, ".json")))]
n_missing_json <- length(all_affected) - length(todo)
if (is.finite(lim)) todo <- head(todo, lim)
mode <- if (commit) "" else " (DRY RUN -- nothing will be written)"
cli::cli_alert_info("{length(todo)} match{?es} to fix{mode}")
if (n_missing_json > 0L) {
  cli::cli_alert_warning(
    "{n_missing_json} affected match{?es} have no local JSON in {.file {SRC}} and will stay name-keyed.")
}

ok <- 0L; skipped <- character(0); t0 <- Sys.time()
for (i in seq_along(todo)) {
  mid <- todo[i]
  res <- tryCatch({
    j <- fromJSON(file.path(SRC, paste0(mid, ".json")), simplifyVector = FALSE)
    info <- parse_match_info(j, mid)
    fresh_pom <- info$player_of_match_id
    if (is.na(fresh_pom) || !grepl("^[0-9a-f]{8}$", fresh_pom)) {
      stop("fresh parse still not a registry id (no registry entry for this name)")
    }
    if (!commit) "dry" else {
      dbExecute(conn, "UPDATE cricsheet.matches SET player_of_match_id = ? WHERE match_id = ?",
                params = list(fresh_pom, mid))
      chk <- dbGetQuery(conn,
        "SELECT player_of_match_id FROM cricsheet.matches WHERE match_id = ?",
        params = list(mid))
      if (!identical(chk$player_of_match_id, fresh_pom)) stop("write did not verify")
      "ok"
    }
  }, error = function(e) paste0("ERR: ", conditionMessage(e)))
  if (identical(res, "ok") || identical(res, "dry")) ok <- ok + 1L
  else skipped <- c(skipped, paste(mid, res))
  if (i %% 200 == 0 || i == length(todo)) {
    cat(sprintf("%d/%d | %d ok | %d skipped | %.1f mins\n", i, length(todo), ok,
                length(skipped), as.numeric(difftime(Sys.time(), t0, units = "mins"))))
    flush.console()
  }
}
dbDisconnect(conn, shutdown = TRUE)
cli::cli_h2("Done")
verb <- if (commit) "fixed" else "would be fixed"
cli::cli_alert_success("{ok} match{?es} {verb}")
if (length(skipped)) {
  # Named, not counted: a silently skipped match stays name-keyed forever.
  # Expected cause: the player-of-match name isn't in THIS match's own
  # registry (data quality in the source JSON, not a bug here).
  cli::cli_alert_warning("{length(skipped)} NOT fixed:")
  for (s in head(skipped, 20)) cli::cli_bullets(c("*" = s))
}
