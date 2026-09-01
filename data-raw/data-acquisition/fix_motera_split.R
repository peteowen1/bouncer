# Un-merge Sardar Patel Stadium (Motera) from Narendra Modi Stadium
# (bouncerverse#73 follow-up, caught by Pete).
#
# #73's coordinate-based crosswalk treated these as the same ground (same
# plot, one geocoded point) and merged them. They are NOT the same ground
# for analytics purposes: the original stadium was demolished in 2015 and a
# completely new structure built on the same plot, reopening Feb 2021 at
# more than double the capacity (54,000 -> 132,000). Different pitch,
# different everything except the address.
#
# Confirmed empirically before fixing, not just from the web search that
# prompted the check: querying the merged "Narendra Modi Stadium, Ahmedabad"
# venue string shows a CLEAN 5.8-year gap with zero matches, 2015-04-24 to
# 2021-02-24 -- exactly the demolition/rebuild window, with real volume on
# both sides (28 matches pre-gap, 69 post). Cricsheet's own original naming
# almost certainly already respected this distinction; the merge broke it.
#
# Fix: split by DATE, not by re-deriving which raw alias string each row
# originally had (that information no longer exists post-merge, but the date
# boundary is unambiguous and matches the real-world rebuild date exactly).
# Every match before the reopening reverts to "Sardar Patel Stadium, Motera";
# everything from the reopening on keeps "Narendra Modi Stadium, Ahmedabad".
#
# Usage: Rscript data-raw/data-acquisition/fix_motera_split.R [--commit]
suppressPackageStartupMessages({
  library(DBI); library(data.table)
  devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE)
})
a <- commandArgs(trailingOnly = TRUE)
commit <- "--commit" %in% a

OLD_NAME <- "Sardar Patel Stadium, Motera"
NEW_NAME <- "Narendra Modi Stadium, Ahmedabad"
REOPEN_DATE <- "2021-02-24"  # first ball at the rebuilt stadium

conn <- get_db_connection(read_only = FALSE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

old_ids <- dbGetQuery(conn, sprintf(
  "SELECT match_id FROM cricsheet.matches WHERE venue = '%s' AND match_date < DATE '%s'",
  NEW_NAME, REOPEN_DATE))$match_id
cli::cli_alert_info("{length(old_ids)} match{?es} to revert to '{OLD_NAME}' (pre-{REOPEN_DATE})")

n_new_remaining <- dbGetQuery(conn, sprintf(
  "SELECT COUNT(*) AS n FROM cricsheet.matches WHERE venue = '%s' AND match_date >= DATE '%s'",
  NEW_NAME, REOPEN_DATE))$n
cli::cli_alert_info("{n_new_remaining} match{?es} stay as '{NEW_NAME}' (>= {REOPEN_DATE})")

if (!length(old_ids)) {
  cli::cli_alert_success("Nothing to revert -- already split, or the merge was already undone.")
  quit(save = "no")
}

if (!commit) {
  cli::cli_alert_info("DRY RUN -- nothing will be written. Pass --commit to apply.")
  quit(save = "no")
}

id_list <- paste(sprintf("'%s'", old_ids), collapse = ",")
DBI::dbBegin(conn)
tryCatch({
  dbExecute(conn, sprintf(
    "UPDATE cricsheet.matches SET venue = '%s' WHERE match_id IN (%s)", OLD_NAME, id_list))
  dbExecute(conn, sprintf(
    "UPDATE cricsheet.deliveries SET venue = '%s' WHERE match_id IN (%s)", OLD_NAME, id_list))
  DBI::dbCommit(conn)
}, error = function(e) { DBI::dbRollback(conn); stop(e) })

# Verify from the data.
still_wrong_m <- dbGetQuery(conn, sprintf(
  "SELECT COUNT(*) AS n FROM cricsheet.matches WHERE match_id IN (%s) AND venue != '%s'",
  id_list, OLD_NAME))$n
still_wrong_d <- dbGetQuery(conn, sprintf(
  "SELECT COUNT(*) AS n FROM cricsheet.deliveries WHERE match_id IN (%s) AND venue != '%s'",
  id_list, OLD_NAME))$n
if (still_wrong_m != 0 || still_wrong_d != 0) {
  cli::cli_abort("Revert did not verify: {still_wrong_m} matches, {still_wrong_d} deliveries still wrong.")
}

# venue_aliases: remove any row that fed the two grounds together. The old
# ground's name becomes its own standalone canonical (no incoming aliases
# needed -- it was a distinct enough string that it wasn't itself an alias
# of anything else).
removed <- dbGetQuery(conn, "SELECT alias, canonical_venue FROM venue_aliases
  WHERE canonical_venue = ? AND (alias LIKE '%Motera%' OR alias LIKE '%Sardar Patel%')",
  params = list(NEW_NAME))
if (nrow(removed)) {
  dbExecute(conn, "DELETE FROM venue_aliases WHERE canonical_venue = ? AND (alias LIKE '%Motera%' OR alias LIKE '%Sardar Patel%')",
            params = list(NEW_NAME))
  cli::cli_alert_success("Removed {nrow(removed)} venue_aliases row{?s} that merged the two grounds:")
  print(removed)
}

cli::cli_alert_success("Reverted {length(old_ids)} match{?es} (matches + deliveries) to '{OLD_NAME}'.")
