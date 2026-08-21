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
dates <- seq(FROM, last, by = paste(BY_MONTHS, "months"))
cli::cli_alert_info("{length(dates)} snapshot date{?s} from {FROM} to {last}, every {BY_MONTHS} month{?s}")
cli::cli_alert_warning("Snapshots are ON OR BEFORE their date; scoring must use one STRICTLY BEFORE the match.")
print(dates)
