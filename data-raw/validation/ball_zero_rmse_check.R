# RMSE rose ~0.4% after adding ball-zero rows. Is that degradation, or is the
# test set simply harder now that it contains the least predictable row in
# cricket -- the final total before a ball is bowled?
setwd("C:/dev/bouncerverse/bouncer")
suppressMessages(devtools::load_all(quiet = TRUE))
suppressMessages({library(xgboost); library(data.table)})
MD <- "C:/dev/bouncerverse/bouncerdata/models"
add_enc <- function(dt) {
  for (ph in c("powerplay","middle","death","new_ball","old_ball")) {
    cn <- paste0("phase_", ph)
    if (!cn %in% names(dt) && "phase" %in% names(dt)) dt[, (cn) := as.integer(phase == ph)]
  }
  if (!"gender_male" %in% names(dt)) dt[, gender_male := as.integer(gender == "male")]
  dt
}
for (fmt in c("t20","odi","test")) {
  res <- readRDS(file.path(MD, sprintf("%s_stage1_results.rds", fmt)))
  fc <- res$feature_cols
  te <- add_enc(as.data.table(readRDS(file.path(MD, sprintf("%s_stage1_data.rds", fmt)))$test))
  if (length(setdiff(fc, names(te)))) { cat(fmt, "cannot score\n"); next }
  p <- predict(res$model, xgb.DMatrix(as.matrix(te[, ..fc])))
  y <- te$final_innings_total
  rm_all <- sqrt(mean((p-y)^2))
  keep <- te$balls_bowled > 0
  rm_ex <- sqrt(mean((p[keep]-y[keep])^2))
  rm_b0 <- if (any(!keep)) sqrt(mean((p[!keep]-y[!keep])^2)) else NA
  cat(sprintf("%-5s all rows %6.2f | excluding ball 0 %6.2f | ball-0 rows only %6.2f (n=%d)\n",
      toupper(fmt), rm_all, rm_ex, rm_b0, sum(!keep)))
}
