# Delete the bare-name fallback junk left over in cricsheet.players after
# #74/#75 (bouncerverse#75, second half).
#
# WHAT THESE ROWS ARE. extract_players() falls back to storing the raw name
# as player_id whenever a match's registry snippet doesn't include that
# player -- the same fallback #74 and #75 fixed for deliveries and
# player_of_match_id. These rows never got the equivalent cleanup: they sit
# in cricsheet.players with player_id == player_name (e.g. "Sara"). Checked
# before writing this script, not assumed: of 3,994 rows referenced by
# nothing (no delivery, no player_of_match_id) after #75's backfill, 3,775
# are this bare-name shape, and 3,278 of THOSE are exact-name duplicates of a
# real player who exists elsewhere under their correct hex id -- dead weight,
# not a second person.
#
# WHAT THIS SCRIPT DELIBERATELY DOES NOT TOUCH. The other 219 unreferenced
# rows are proper 8-hex-char ids: real, correctly-resolved players who are
# just genuinely inactive (named in a squad, never recorded a delivery or won
# player-of-match). Nothing wrong with those -- deleting them would be
# removing real registry data with no correctness justification, a different
# operation from clearing fallback junk. Left alone on purpose.
#
# Usage: Rscript data-raw/data-acquisition/delete_orphan_player_names.R
suppressPackageStartupMessages({
  library(DBI)
  devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE)
})
conn <- get_db_connection(read_only = FALSE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

before <- dbGetQuery(conn, "SELECT COUNT(*) AS n FROM cricsheet.players")$n

target <- dbGetQuery(conn, "
  WITH ref_deliv AS (SELECT DISTINCT batter_id AS pid FROM cricsheet.deliveries
                      UNION SELECT DISTINCT bowler_id FROM cricsheet.deliveries
                      UNION SELECT DISTINCT non_striker_id FROM cricsheet.deliveries),
       ref_pom AS (SELECT DISTINCT player_of_match_id AS pid FROM cricsheet.matches WHERE player_of_match_id IS NOT NULL)
  SELECT player_id FROM cricsheet.players p
  WHERE p.player_id NOT IN (SELECT pid FROM ref_deliv)
    AND p.player_id NOT IN (SELECT pid FROM ref_pom)
    AND NOT regexp_matches(p.player_id, '^[0-9a-f]{8}$')")$player_id

cat(sprintf("before: %d rows | targeting %d bare-name fallback rows for deletion\n", before, length(target)))

duckdb::duckdb_register(conn, "orphan_target", data.frame(player_id = target))
n <- DBI::dbExecute(conn, "DELETE FROM cricsheet.players WHERE player_id IN (SELECT player_id FROM orphan_target)")
DBI::dbExecute(conn, "DROP VIEW IF EXISTS orphan_target")

after <- dbGetQuery(conn, "SELECT COUNT(*) AS n FROM cricsheet.players")$n
cat(sprintf("deleted: %d | after: %d rows (expected %d)\n", n, after, before - length(target)))
stopifnot(after == before - length(target))

# Post-check: every remaining row is either referenced or a proper hex id
# with no bug-class explanation for its absence.
remaining_orphans <- dbGetQuery(conn, "
  WITH ref_deliv AS (SELECT DISTINCT batter_id AS pid FROM cricsheet.deliveries
                      UNION SELECT DISTINCT bowler_id FROM cricsheet.deliveries
                      UNION SELECT DISTINCT non_striker_id FROM cricsheet.deliveries),
       ref_pom AS (SELECT DISTINCT player_of_match_id AS pid FROM cricsheet.matches WHERE player_of_match_id IS NOT NULL)
  SELECT COUNT(*) AS n,
    SUM(CASE WHEN regexp_matches(player_id,'^[0-9a-f]{8}$') THEN 0 ELSE 1 END) AS still_bare_name
  FROM cricsheet.players p
  WHERE p.player_id NOT IN (SELECT pid FROM ref_deliv) AND p.player_id NOT IN (SELECT pid FROM ref_pom)")
print(remaining_orphans)
