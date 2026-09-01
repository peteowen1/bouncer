# Canonicalize aliased venue names in the two SOURCE tables (bouncerverse#73).
#
# 22 tables in the DB carry a `venue` column; 20 of them are downstream/
# derived (3-way ELO x6, venue skill x3, pre_match_features, weather joins,
# venue_coordinates itself) and get REBUILT by their own pipeline scripts --
# hand-patching a derived table is exactly the kind of drift #45 warns
# about. Only cricsheet.matches and cricsheet.deliveries are source: both
# carry the raw cricsheet venue string (deliveries denormalizes it per ball),
# so both must move together or they go out of sync with each other.
#
# Uses docs/reference/D73-VENUE-CROSSWALK-CANDIDATE.csv (68 alias ->
# canonical mappings), built from coordinate collisions among Test venues
# and disposed by hand -- see that file's own issue, #73, for the reasoning
# on what's excluded (Colombo's genuinely-distinct grounds; Bangabandhu/
# Shere Bangla, which geocode identically and need a human check before any
# merge, not assumed here).
#
# Scoped to Test venues only (that's what the crosswalk was built from) --
# does NOT touch T20/ODI venue names, which were never audited for the same
# alias problem.
#
# RESUMABLE and per-transaction the same way #74/#75's fixes were: each
# alias->canonical pair commits as one update across both tables, verified
# by re-querying rather than trusting the row count DuckDB returns.
#
# Usage: Rscript data-raw/data-acquisition/fix_73_venue_aliases.R [--commit]
suppressPackageStartupMessages({
  library(DBI); library(data.table)
  devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE)
})

a <- commandArgs(trailingOnly = TRUE)
commit <- "--commit" %in% a

cw_path <- file.path(find_bouncerdata_dir(), "..", "docs", "reference",
                     "D73-VENUE-CROSSWALK-CANDIDATE.csv")
cw_path <- normalizePath(cw_path, mustWork = FALSE)
if (!file.exists(cw_path)) {
  # bouncerverse sits alongside bouncer/bouncerdata, not inside them.
  cw_path <- "C:/dev/bouncerverse/docs/reference/D73-VENUE-CROSSWALK-CANDIDATE.csv"
}
stopifnot(file.exists(cw_path))
cw <- fread(cw_path)
cw <- cw[alias != canonical]
cli::cli_alert_info("{nrow(cw)} alias -> canonical mappings loaded")

conn <- get_db_connection(read_only = FALSE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

mode <- if (commit) "" else " (DRY RUN -- nothing will be written)"
cli::cli_h1("Venue alias fix{mode}")

ok <- 0L; skipped <- character(0)
for (i in seq_len(nrow(cw))) {
  alias <- cw$alias[i]; canon <- cw$canonical[i]

  n_matches <- dbGetQuery(conn,
    "SELECT COUNT(*) AS n FROM cricsheet.matches WHERE venue = ?", params = list(alias))$n
  n_deliv <- dbGetQuery(conn,
    "SELECT COUNT(*) AS n FROM cricsheet.deliveries WHERE venue = ?", params = list(alias))$n

  if (n_matches == 0 && n_deliv == 0) {
    skipped <- c(skipped, sprintf("%s: 0 rows in either table (already clean or never existed)", alias))
    next
  }

  res <- tryCatch({
    if (!commit) "dry" else {
      DBI::dbBegin(conn)
      tryCatch({
        dbExecute(conn, "UPDATE cricsheet.matches SET venue = ? WHERE venue = ?",
                  params = list(canon, alias))
        dbExecute(conn, "UPDATE cricsheet.deliveries SET venue = ? WHERE venue = ?",
                  params = list(canon, alias))
        DBI::dbCommit(conn)
      }, error = function(e) { DBI::dbRollback(conn); stop(e) })

      # Verify from the data, not from dbExecute's return.
      still_m <- dbGetQuery(conn, "SELECT COUNT(*) AS n FROM cricsheet.matches WHERE venue = ?",
                            params = list(alias))$n
      still_d <- dbGetQuery(conn, "SELECT COUNT(*) AS n FROM cricsheet.deliveries WHERE venue = ?",
                            params = list(alias))$n
      if (still_m != 0 || still_d != 0) stop("rows still under the alias after update")
      "ok"
    }
  }, error = function(e) paste0("ERR: ", conditionMessage(e)))

  if (identical(res, "ok") || identical(res, "dry")) {
    ok <- ok + 1L
    cli::cli_alert_success("{alias} -> {canon} ({n_matches} matches, {n_deliv} deliveries){if (!commit) ' [dry]'}")
  } else {
    skipped <- c(skipped, paste(alias, res))
  }
}

cli::cli_h2("Done")
verb <- if (commit) "fixed" else "would be fixed"
cli::cli_alert_success("{ok} alias{?es} {verb}")
if (length(skipped)) {
  cli::cli_alert_warning("{length(skipped)} not fixed / skipped:")
  for (s in skipped) cli::cli_bullets(c("*" = s))
}
