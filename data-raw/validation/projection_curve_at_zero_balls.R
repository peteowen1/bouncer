# Why is first-ball TSA still -1.2 rather than ~0?
#
# Hypothesis: the BEFORE-state of ball one is 0 balls bowled, and training
# contains NO 0-ball rows -- the model has never seen that state. Fixing the
# run-rate degeneracy did not fix the fact that the state itself is
# out-of-distribution.
setwd("C:/dev/bouncerverse/bouncer")
suppressMessages(devtools::load_all(quiet = TRUE))
suppressMessages({library(data.table)})
options(bouncer.warn_momentum_impute = FALSE)

cat("=== 1. confirm training still has no 0-ball rows ===\n")
for (fmt in c("t20","odi")) {
  d <- as.data.table(readRDS(sprintf("C:/dev/bouncerverse/bouncerdata/models/%s_stage1_data.rds", fmt))$train)
  cat(sprintf("%s: min balls_bowled = %d | rows at 0 = %d\n", toupper(fmt),
      min(d$balls_bowled), sum(d$balls_bowled == 0)))
}

cat("\n=== 2. is the projection at 0 balls off the curve? ===\n")
cat("Score 0 runs / 0 wickets at increasing ball counts. A smooth model should\n")
cat("give a gently DECLINING projection as balls are consumed without runs.\n\n")
for (fmt in c("t20","odi")) {
  models <- load_in_match_models(fmt)
  bb <- c(0, 1, 2, 3, 6, 12, 24)
  s <- data.frame(current_score = 0, wickets = 0, overs = (bb %/% 6) + (bb %% 6)/10,
                  innings = 1L, target = NA_real_, gender_male = 1L,
                  venue_avg_score = if (fmt=="t20") 160 else 250,
                  venue_chase_success_rate = 0.5,
                  venue_avg_second_innings = if (fmt=="t20") 150 else 240,
                  innings1_wickets = NA_real_)
  p <- predict_win_probability_batch(s, format = fmt, models = models, detail = TRUE)$projected_score
  cat(sprintf("%s:\n", toupper(fmt)))
  for (i in seq_along(bb)) cat(sprintf("  %2d balls, 0/0 -> %7.2f%s\n", bb[i], p[i],
      if (i > 1) sprintf("   (step %+.2f)", p[i]-p[i-1]) else ""))
  cat(sprintf("  step 0->1 is %+.2f; steps 1->2, 2->3 are %+.2f, %+.2f\n\n",
      p[2]-p[1], p[3]-p[2], p[4]-p[3]))
}
