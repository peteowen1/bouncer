# Standing check: every player reference must be a registry id, never a name.
#
# bouncerverse#74. For essentially all of 2026 -- 0.00% before 2026-01-01 and
# 71-100% after -- cricsheet.deliveries stored player NAMES instead of registry
# ids, splitting every current player into a second, low-exposure identity.
# 995 matches, 413,259 deliveries, 3,139 phantom players. Nothing failed, for
# months, because the parser's fallback was silent.
#
# This is the check that would have caught it the day it started. Run it after
# any ingestion.
#
# Usage: Rscript data-raw/validation/player_id_integrity.R
suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages(library(DBI))

HEX8 <- "'^[0-9a-f]{8}$'"
conn <- get_db_connection(read_only = TRUE)
on.exit(dbDisconnect(conn, shutdown = TRUE), add = TRUE)

cli::cli_h1("Player id integrity")

fail <- character(0)

# 1. The delivery columns every rating keys on.
cols <- c("batter_id", "bowler_id", "non_striker_id")
q <- dbGetQuery(conn, sprintf("
  SELECT COUNT(*) AS total, %s FROM cricsheet.deliveries",
  paste(sprintf("SUM(CASE WHEN %s IS NULL OR regexp_matches(%s, %s) THEN 0 ELSE 1 END) AS %s_names",
                cols, cols, HEX8, cols), collapse = ", ")))
cli::cli_alert_info("{format(q$total, big.mark = ',')} deliveries")
for (cl in cols) {
  n <- q[[paste0(cl, "_names")]]
  if (n > 0) {
    fail <- c(fail, sprintf("%s: %s name-keyed", cl, format(n, big.mark = ",")))
    cli::cli_alert_danger("{cl}: {format(n, big.mark = ',')} name-keyed")
  } else {
    cli::cli_alert_success("{cl}: all registry ids")
  }
}

# 2. By year, because the failure mode is a REGRESSION -- a share that is fine
#    overall can be total in the current season and still look small diluted
#    across twenty years of history.
cli::cli_h2("By year (deliveries)")
yr <- dbGetQuery(conn, sprintf("
  SELECT YEAR(match_date) AS yr, COUNT(*) AS balls,
         ROUND(100.0 * SUM(CASE WHEN regexp_matches(batter_id, %s) THEN 0 ELSE 1 END)
               / COUNT(*), 2) AS pct_name
  FROM cricsheet.deliveries WHERE match_date >= '2023-01-01'
  GROUP BY 1 ORDER BY 1", HEX8))
print(yr, row.names = FALSE)
if (any(yr$pct_name > 0)) {
  bad <- yr[yr$pct_name > 0, ]
  fail <- c(fail, sprintf("year %s at %.2f%%", bad$yr, bad$pct_name))
}

# 3. Known separate defects, REPORTED not asserted, so this check stays green
#    for the thing it is actually guarding and does not become noise.
cli::cli_h2("Known, tracked separately")
pom <- dbGetQuery(conn, sprintf("
  SELECT COUNT(*) AS n FROM cricsheet.matches
  WHERE player_of_match_id IS NOT NULL AND NOT regexp_matches(player_of_match_id, %s)", HEX8))$n
cli::cli_alert_warning("matches.player_of_match_id name-keyed: {format(pom, big.mark = ',')} (63-77% every year since 2020, predates #74)")
orph <- dbGetQuery(conn, sprintf("
  SELECT COUNT(*) AS n FROM cricsheet.players p
  WHERE NOT regexp_matches(p.player_id, %s)
    AND NOT EXISTS (SELECT 1 FROM cricsheet.deliveries d
                    WHERE d.batter_id = p.player_id OR d.bowler_id = p.player_id)", HEX8))$n
cli::cli_alert_warning("orphaned name-keyed rows in cricsheet.players: {format(orph, big.mark = ',')} (needs a merge rule, not a delete)")

if (length(fail)) {
  cli::cli_abort(c("Player id integrity FAILED.",
                   stats::setNames(fail, rep("x", length(fail))),
                   "i" = "See bouncerverse#74 and data-raw/data-acquisition/fix_74_player_ids.R."))
}
cli::cli_alert_success("Every delivery carries a registry id.")
