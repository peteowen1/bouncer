# Show optimized women's parameters
cat("=== Women's Optimized Parameters ===\n\n")

for (type in c("run", "wicket")) {
  for (fmt in c("t20", "odi", "test")) {
    f <- sprintf("../bouncerdata/models/%s_elo_params_womens_%s.rds", type, fmt)
    if (file.exists(f)) {
      r <- readRDS(f)
      cat(sprintf("Womens %s %s:\n", toupper(fmt), toupper(type)))
      cat(sprintf("  w_batter: %.3f, w_bowler: %.3f\n",
                  r$optimized_params["w_batter"], r$optimized_params["w_bowler"]))
      if (type == "run") {
        cat(sprintf("  runs_per_100_elo: %.4f\n", r$optimized_params["runs_per_100_elo"]))
      }
      cat("\n")
    }
  }
}
