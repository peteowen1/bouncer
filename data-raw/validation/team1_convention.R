# #30: is "team1" the side batting first, everywhere?
#
# Training used cricsheet's LISTED matches.team1 for the label and for
# batting_is_team1, while team1_completed/team2_completed attributed innings
# 1+3 to one side and 2+4 to the other -- the batting-order alternation. Those
# agree only when the listed team1 actually bats first.
setwd("C:/dev/bouncerverse/bouncer")
suppressMessages(devtools::load_all(quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
conn <- get_db_connection(read_only = TRUE); on.exit(dbDisconnect(conn, shutdown = TRUE))

d <- as.data.table(dbGetQuery(conn, "
  SELECT m.match_id, m.match_type, m.team1, m.outcome_winner, m.outcome_type,
         MIN(d.batting_team) FILTER (WHERE d.innings = 1) AS inn1_batting
  FROM cricsheet.matches m JOIN cricsheet.deliveries d ON d.match_id = m.match_id
  WHERE LOWER(m.match_type) IN ('test','mdm') AND m.outcome_type IS NOT NULL
  GROUP BY 1,2,3,4,5"))

cat(sprintf("matches with an outcome and innings-1 deliveries: %d\n\n", nrow(d)))
d[, aligned := team1 == inn1_batting]

cat("=== the OLD convention: how often was the listed team1 the side batting first? ===\n")
print(d[, .(matches = .N, listed_team1_batted_first_pct = round(100*mean(aligned), 1)),
        by = match_type][order(-matches)])
cat(sprintf("\noverall: %.1f%% aligned, so %d matches (%.1f%%) carried an inverted label\n",
    100*mean(d$aligned), sum(!d$aligned), 100*mean(!d$aligned)))

cat("\n=== the NEW convention: team1 := inn1_batting, by construction ===\n")
cat(sprintf("alignment of the new label with the batting-order alternation: %.1f%%\n",
    100*mean(d$inn1_batting == d$inn1_batting)))

cat("\n=== does it matter for the LABEL? ===\n")
d[, old_label := fcase(outcome_type == "draw", 1L, outcome_winner == team1, 0L, default = 2L)]
d[, new_label := fcase(outcome_type == "draw", 1L, outcome_winner == inn1_batting, 0L, default = 2L)]
flip <- d[old_label != new_label]
cat(sprintf("labels that CHANGE: %d of %d (%.1f%%)\n", nrow(flip), nrow(d), 100*nrow(flip)/nrow(d)))
print(flip[, .(matches = .N), by = match_type])

cat("\n=== is the holdout period representative? ===\n")
dates <- as.data.table(dbGetQuery(conn, "
  SELECT match_id, CAST(match_date AS DATE) AS md FROM cricsheet.matches"))
d <- merge(d, dates, by = "match_id", all.x = TRUE)
d[, era := fifelse(md >= as.Date('2023-09-01'), "holdout 2023/24+", "training <2023/24")]
print(d[, .(matches = .N, misaligned = sum(!aligned),
            pct = round(100*mean(!aligned), 1)), by = era][order(era)])
cat("\nThe ticket's point: the misalignment is concentrated where the holdout cannot see it.\n")
