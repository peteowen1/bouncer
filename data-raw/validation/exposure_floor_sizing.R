# #57 part 1: what does min_balls = 500 actually mean per format?
#
# The ticket's premise: 500 balls is 25 T20 matches but only 10 ODIs, so one
# number cannot be the right floor for three formats. Size it before tuning --
# this family of knobs has historically been worth under 1%.
setwd("C:/dev/bouncerverse/bouncer")
suppressMessages(devtools::load_all(quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
conn <- get_db_connection(read_only = TRUE); on.exit(dbDisconnect(conn, shutdown=TRUE))

b <- as.data.table(dbGetQuery(conn, "
  SELECT r.format, r.gender, r.batter_id, r.bowler_id, r.match_id, r.raa
  FROM main.cricsheet_ball_raa r WHERE r.gender = 'male'"))
cat(sprintf("balls: %s\n\n", format(nrow(b), big.mark=",")))

cat("=== what 500 balls buys you, per format ===\n")
for (role in c("batter","bowler")) {
  id <- paste0(role, "_id")
  x <- b[, .(balls = .N, matches = uniqueN(match_id)), by = c("format", id)]
  s <- x[balls >= 100, .(players = .N,
                         median_balls_per_match = round(median(balls/matches), 1),
                         matches_for_500_balls = round(500 / median(balls/matches), 1)),
         by = format]
  cat(sprintf("\n%s:\n", role)); print(s[order(format)])
}
