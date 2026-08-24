# The full XI per team per match, from cricsheet's own info.players.
#
# WHY THIS EXISTS. #60 decided the team rating would be composed from "who
# actually appeared", on the stated grounds that the XI was mostly not
# recoverable -- T20 deliveries show only 15.8 batters of 22, and
# t20_player_game_data covers 1,977 of 14,130 matches. Both true, and both
# irrelevant: cricsheet's JSON carries `info.players` as an explicit named
# eleven per side. The parser reads it (extract_players) but keeps only the
# unique players for the registry, discarding the team assignment, so nothing
# downstream could see a squad.
#
# WHY IT MATTERS. Composing from appearances injects the RESULT into the
# feature: the count of players who appear correlates -0.558 (T20) and -0.601
# (ODI) with the margin, because a side bowled out uses eleven batters and a
# side chasing comfortably uses five. A squad taken from info.players is fixed
# before a ball is bowled and cannot know the outcome.
#
# Usage: Rscript data-raw/data-acquisition/build_match_squads.R <json_dir>
suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(jsonlite); library(data.table)})

SRC <- commandArgs(trailingOnly = TRUE)[1]
if (is.na(SRC) || !dir.exists(SRC)) cli::cli_abort("Pass a directory of cricsheet JSON files.")
files <- list.files(SRC, pattern = "[.]json$", full.names = TRUE)
cli::cli_alert_info("{format(length(files), big.mark = ',')} JSON file{?s} in {.file {SRC}}")

TBL <- "match_squads"
conn <- get_db_connection(read_only = FALSE)
on.exit(try(dbDisconnect(conn, shutdown = TRUE), silent = TRUE), add = TRUE)
if (!table_exists(conn, TBL)) {
  dbExecute(conn, sprintf("CREATE TABLE %s (
    match_id VARCHAR, team VARCHAR, player_id VARCHAR, player_name VARCHAR,
    from_registry BOOLEAN)", TBL))
  cli::cli_alert_success("Created {TBL}")
}
# "Done" means BOTH teams landed, not merely that the match_id appears.
#
# The flush is a buffered dbWriteTable(append = TRUE) with no explicit
# transaction. A DuckDB append should commit or roll back as a unit, so a
# half-written match should not be possible -- but that rests on Appender
# semantics rather than on anything this script guarantees, and the cost of
# being wrong is a match permanently skipped as complete while missing a side.
# Requiring two teams closes it for the price of a GROUP BY.
done <- dbGetQuery(conn, sprintf(
  "SELECT match_id FROM %s GROUP BY match_id HAVING COUNT(DISTINCT team) >= 2", TBL))$match_id
todo <- files[!(tools::file_path_sans_ext(basename(files)) %in% done)]
cli::cli_alert_info("{format(length(done), big.mark=',')} already done; {format(length(todo), big.mark=',')} to do")

buf <- list(); ok <- 0L; no_players <- character(0); t0 <- Sys.time()
flush_buf <- function() {
  if (!length(buf)) return(invisible(0L))
  d <- rbindlist(buf, fill = TRUE)
  dbWriteTable(conn, TBL, as.data.frame(d), append = TRUE)
  buf <<- list(); invisible(nrow(d))
}
for (i in seq_along(todo)) {
  mid <- tools::file_path_sans_ext(basename(todo[i]))
  res <- tryCatch({
    j <- fromJSON(todo[i], simplifyVector = FALSE)
    pl <- j$info$players
    if (is.null(pl) || !length(pl)) stop("no info$players")
    reg <- j$info$registry$people
    rows <- rbindlist(lapply(names(pl), function(tm) {
      nms <- unlist(pl[[tm]])
      ids <- vapply(nms, function(n)
        if (!is.null(reg) && n %in% names(reg)) reg[[n]] else n, character(1))
      data.table(match_id = mid, team = tm, player_id = unname(ids),
                 player_name = nms,
                 # Flagged, not silently equal: a name standing in for an id is
                 # the #74 defect, and a squad row is where it would re-enter.
                 from_registry = unname(ids) != nms)
    }))
    # A match that never reaches 2 distinct teams (partial JSON, walkover)
    # never satisfies the done-check above and gets reprocessed every rerun.
    # Delete before append so that stays idempotent instead of duplicating rows.
    dbExecute(conn, sprintf("DELETE FROM %s WHERE match_id = ?", TBL), params = list(mid))
    buf[[length(buf) + 1L]] <- rows
    ok <- ok + 1L
    "ok"
  }, error = function(e) paste0("ERR: ", conditionMessage(e)))
  if (!identical(res, "ok")) no_players <- c(no_players, paste(mid, res))
  if (length(buf) >= 500L || i == length(todo)) flush_buf()
  if (i %% 2000 == 0 || i == length(todo)) {
    cat(sprintf("%d/%d | %d ok | %d skipped | %.1f mins\n", i, length(todo), ok,
                length(no_players), as.numeric(difftime(Sys.time(), t0, units = "mins"))))
    flush.console()
  }
}
q <- dbGetQuery(conn, sprintf("SELECT COUNT(*) n, COUNT(DISTINCT match_id) m,
  SUM(CASE WHEN from_registry THEN 0 ELSE 1 END) name_keyed FROM %s", TBL))
cli::cli_h2("Done")
cli::cli_alert_success("{format(q$n, big.mark=',')} squad row{?s} across {format(q$m, big.mark=',')} match{?es}")
if (q$name_keyed > 0) {
  cli::cli_alert_warning("{format(q$name_keyed, big.mark=',')} row{?s} carry a NAME, not a registry id (see #74).")
}
if (length(no_players)) {
  cli::cli_alert_warning("{length(no_players)} file{?s} had no usable squad:")
  for (f in head(no_players, 10)) cli::cli_bullets(c("*" = f))
}
