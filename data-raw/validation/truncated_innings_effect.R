setwd("C:/dev/bouncerverse/bouncer")
suppressMessages(devtools::load_all(quiet = TRUE))
suppressMessages({library(data.table)})
MD <- "C:/dev/bouncerverse/bouncerdata/models"
cuts <- list(t20 = 108, odi = 270)   # 90% of a full innings: 18 and 45 overs
for (fmt in c("t20","odi")) {
  te <- as.data.table(readRDS(file.path(MD, sprintf("%s_stage1_data.rds", fmt)))$test)
  inn <- te[, .(balls = .N, total = max(final_innings_total),
                wkts = max(wickets_fallen)), by = match_id]
  full <- inn[balls >= cuts[[fmt]] | wkts >= 10]
  trunc <- inn[balls < cuts[[fmt]] & wkts < 10]
  cat(sprintf("\n%s test innings: %d\n", toupper(fmt), nrow(inn)))
  cat(sprintf("  completed (full length OR all out): %4d  mean %6.1f\n", nrow(full), mean(full$total)))
  cat(sprintf("  truncated (short AND not all out):  %4d  mean %6.1f\n", nrow(trunc), mean(trunc$total)))
  cat(sprintf("  overall mean %6.1f | completed-only mean %6.1f | truncation pulls it down %.1f runs\n",
      mean(inn$total), mean(full$total), mean(full$total) - mean(inn$total)))
}
