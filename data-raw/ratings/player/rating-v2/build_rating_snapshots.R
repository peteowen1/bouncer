# Time-causal rating snapshots, so a team rating can be scored honestly
# (bouncerverse#61).
#
# THE PROBLEM THIS SOLVES. main.player_value_v2 is one snapshot fitted on the
# whole corpus. Scoring held-out matches with it asks a rating that already
# contains their outcome to predict them, and the result-ELO it is compared
# against updates strictly forward and cannot cheat -- so the comparison is
# rigged before any modelling happens. #29 and #69 are the same defect: a
# feature that WAS the label.
#
# THE SHARP EDGE. calculate_player_rating_v2(as_at = D) filters
# `d.match_date <= DATE 'D'` -- ON OR BEFORE. So a snapshot dated D includes
# matches played on D, and using it to score a match on D leaks that match.
# Scoring must therefore pick the latest snapshot STRICTLY BEFORE the match
# date, which is what pick_snapshot() below enforces.
#
# COST. One build per (snapshot date, format, gender, role). Time a single
# build before launching the grid -- see SIZING below. Cadence is a deliberate
# trade: finer snapshots are more causally tight and cost linearly more.
#
# Usage:
#   Rscript build_rating_snapshots.R --sizing          # time one build, exit
#   Rscript build_rating_snapshots.R --from 2023-01-01 --by 6   # months
suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})

a <- commandArgs(trailingOnly = TRUE)
getopt <- function(flag, default) {
  i <- which(a == flag); if (length(i)) a[i + 1L] else default
}
FROM <- as.Date(getopt("--from", "2023-01-01"))
BY_MONTHS <- as.integer(getopt("--by", "6"))
SIZING <- "--sizing" %in% a

conn <- get_db_connection(read_only = TRUE)
on.exit(dbDisconnect(conn, shutdown = TRUE), add = TRUE)

if (SIZING) {
  cli::cli_h2("Sizing: one rating build")
  t0 <- Sys.time()
  r <- calculate_player_rating_v2("t20", "male", "batter", conn = conn,
                                  as_at = FROM)
  secs <- as.numeric(difftime(Sys.time(), t0, units = "secs"))
  cli::cli_alert_info("t20/male/batter as at {FROM}: {nrow(r)} rated in {round(secs,1)}s")
  # 3 formats x 2 genders x 2 roles = 12 builds per snapshot date.
  cli::cli_alert_info("=> ~{round(12 * secs / 60, 1)} min per snapshot date, all formats/genders/roles")
  quit(status = 0)
}

last <- as.Date(dbGetQuery(conn, "SELECT MAX(match_date) AS d FROM cricsheet.matches")$d)
dbDisconnect(conn, shutdown = TRUE)

dates <- seq(FROM, last, by = paste(BY_MONTHS, "months"))
cli::cli_alert_info("{length(dates)} snapshot date{?s} from {FROM} to {last}, every {BY_MONTHS} month{?s}")
cli::cli_alert_warning("Snapshots are ON OR BEFORE their date; scoring must use one STRICTLY BEFORE the match (pick_snapshot()).")

GRID <- expand.grid(as_at = dates,
                    format = c("t20", "odi", "test"),
                    gender = c("male", "female"),
                    role = c("batter", "bowler"),
                    stringsAsFactors = FALSE)
cli::cli_alert_info("{nrow(GRID)} builds at ~19s each: roughly {round(nrow(GRID) * 19 / 60)} minutes")

TBL <- "player_rating_v2_snapshots"
conn <- get_db_connection(read_only = FALSE)
on.exit(try(dbDisconnect(conn, shutdown = TRUE), silent = TRUE), add = TRUE)

if (!table_exists(conn, TBL)) {
  dbExecute(conn, sprintf("CREATE TABLE %s (
      as_at DATE, format VARCHAR, gender VARCHAR, role VARCHAR,
      player_id VARCHAR, player_name VARCHAR, rating DOUBLE,
      matches INTEGER, balls INTEGER, built_at TIMESTAMP)", TBL))
  cli::cli_alert_success("Created {TBL}")
}

# RESUMABLE. Every long job in this session has been interrupted at least
# once, and a grid that restarts from zero each time never finishes. Each
# (as_at, format, gender, role) commits on its own and is skipped if already
# present, so a rerun does only what is missing.
done <- dbGetQuery(conn, sprintf(
  "SELECT DISTINCT as_at, format, gender, role FROM %s", TBL))
key <- function(d) paste(as.character(d$as_at), d$format, d$gender, d$role)
todo <- GRID[!(key(GRID) %in% key(done)), , drop = FALSE]
cli::cli_alert_info("{nrow(done)} combination{?s} already built; {nrow(todo)} to do")

ok <- 0L; failed <- character(0); t0 <- Sys.time()
for (i in seq_len(nrow(todo))) {
  g <- todo[i, ]
  res <- tryCatch({
    r <- calculate_player_rating_v2(g$format, g$gender, g$role, conn = conn,
                                    as_at = as.Date(g$as_at))
    if (is.null(r) || !nrow(r)) stop("no players rated")
    keep <- data.frame(
      as_at = as.Date(g$as_at), format = g$format, gender = g$gender, role = g$role,
      player_id = r$player_id, player_name = r$player_name, rating = r$rating,
      matches = as.integer(r$matches), balls = as.integer(r$balls),
      built_at = Sys.time(), stringsAsFactors = FALSE)
    .in_transaction(conn, function() {
      dbExecute(conn, sprintf(
        "DELETE FROM %s WHERE as_at = DATE '%s' AND format = '%s' AND gender = '%s' AND role = '%s'",
        TBL, as.Date(g$as_at), g$format, g$gender, g$role))
      dbWriteTable(conn, TBL, keep, append = TRUE)
    })
    "ok"
  }, error = function(e) paste0("ERR: ", conditionMessage(e)))
  if (identical(res, "ok")) ok <- ok + 1L else
    failed <- c(failed, sprintf("%s %s/%s/%s %s", g$as_at, g$format, g$gender, g$role, res))
  if (i %% 5 == 0 || i == nrow(todo)) {
    cat(sprintf("%d/%d | %d ok | %d failed | %.1f mins
", i, nrow(todo), ok,
                length(failed), as.numeric(difftime(Sys.time(), t0, units = "mins"))))
    flush.console()
  }
}

cli::cli_h2("Done")
cli::cli_alert_success("{ok} build{?s} stored")
if (length(failed)) {
  # NAMED, not counted -- a silently missing snapshot becomes a silently
  # unscorable stretch of matches later.
  cli::cli_alert_warning("{length(failed)} failed:")
  for (f in head(failed, 25)) cli::cli_bullets(c("*" = f))
}
