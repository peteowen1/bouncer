# Redo the truncation effect using SCORECARD wickets, not the frame's
# pre-delivery wickets_fallen -- whose max is 9 for an all-out innings, so my
# earlier cut counted every all-out innings as "truncated".
setwd("C:/dev/bouncerverse/bouncer")
suppressMessages(devtools::load_all(quiet = TRUE))
suppressMessages(library(data.table))
conn <- get_db_connection(read_only = TRUE)
card <- as.data.table(DBI::dbGetQuery(conn, "
  SELECT mi.match_id, mi.total_runs AS card_runs, mi.total_wickets AS card_wkts
  FROM cricsheet.match_innings mi WHERE mi.innings = 1"))
DBI::dbDisconnect(conn, shutdown = TRUE)
MD <- "C:/dev/bouncerverse/bouncerdata/models"
for (fmt in c("t20","odi")) {
  te <- as.data.table(readRDS(file.path(MD, sprintf("%s_stage1_data.rds", fmt)))$test)
  inn <- te[balls_bowled > 0, .(balls = .N, total = max(final_innings_total)), by = match_id]
  inn <- merge(inn, card, by = "match_id")
  sched <- if (fmt == "t20") 120 else 300
  inn[, truncated := balls < 0.9*sched & card_wkts < 10]
  cat(sprintf("\n%s test innings: %d\n", toupper(fmt), nrow(inn)))
  cat(sprintf("  all out            : %4d (%4.1f%%)  mean %6.1f\n",
      inn[card_wkts >= 10, .N], 100*mean(inn$card_wkts >= 10), inn[card_wkts >= 10, mean(total)]))
  cat(sprintf("  TRULY truncated    : %4d (%4.1f%%)  mean %6.1f\n",
      inn[truncated == TRUE, .N], 100*mean(inn$truncated), inn[truncated == TRUE, mean(total)]))
  cat(sprintf("  completed normally : %4d (%4.1f%%)  mean %6.1f\n",
      inn[truncated == FALSE, .N], 100*mean(!inn$truncated), inn[truncated == FALSE, mean(total)]))
  cat(sprintf("  overall mean %6.1f | non-truncated mean %6.1f | truncation pulls it down %.1f runs\n",
      mean(inn$total), inn[truncated == FALSE, mean(total)],
      inn[truncated == FALSE, mean(total)] - mean(inn$total)))
}
