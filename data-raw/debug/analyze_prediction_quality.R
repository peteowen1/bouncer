# Analyze prediction quality with proper metrics
library(DBI)
library(data.table)
devtools::load_all()

conn <- get_db_connection(read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

tables <- c(
  "mens_t20_3way_elo" = "Men's T20",
  "mens_odi_3way_elo" = "Men's ODI",
  "mens_test_3way_elo" = "Men's Test",
  "womens_t20_3way_elo" = "Women's T20",
  "womens_odi_3way_elo" = "Women's ODI",
  "womens_test_3way_elo" = "Women's Test"
)

cat("\n")
cli::cli_h1("Wicket Prediction Quality Analysis")
cat("\n")

results <- list()

for (i in seq_along(tables)) {
  tbl <- names(tables)[i]
  label <- tables[i]

  stats <- DBI::dbGetQuery(conn, sprintf("
    SELECT
      COUNT(*) as n,
      AVG(CASE WHEN is_wicket THEN 1.0 ELSE 0.0 END) as wicket_rate,
      AVG(exp_wicket) as mean_pred,
      -- Log loss
      -AVG(
        CASE WHEN is_wicket
          THEN LN(GREATEST(exp_wicket, 0.001))
          ELSE LN(GREATEST(1 - exp_wicket, 0.001))
        END
      ) as log_loss,
      -- Brier score
      AVG(POWER(exp_wicket - CASE WHEN is_wicket THEN 1.0 ELSE 0.0 END, 2)) as brier_score
    FROM %s
  ", tbl))

  # Baseline metrics (always predict the mean rate)
  base_rate <- stats$wicket_rate
  baseline_logloss <- -(base_rate * log(max(base_rate, 0.001)) + (1-base_rate) * log(max(1-base_rate, 0.001)))
  baseline_brier <- base_rate * (1 - base_rate)

  # Skill scores (% improvement over baseline)
  logloss_skill <- (1 - (stats$log_loss / baseline_logloss)) * 100
  brier_skill <- (1 - (stats$brier_score / baseline_brier)) * 100

  cat(sprintf("\n--- %s ---\n", label))
  cat(sprintf("Actual wicket rate:    %.2f%%\n", stats$wicket_rate * 100))
  cat(sprintf("Mean predicted:        %.2f%%\n", stats$mean_pred * 100))
  cat(sprintf("Log Loss:              %.4f (baseline: %.4f)\n", stats$log_loss, baseline_logloss))
  cat(sprintf("  -> Skill:            %.2f%% improvement over baseline\n", logloss_skill))
  cat(sprintf("Brier Score:           %.5f (baseline: %.5f)\n", stats$brier_score, baseline_brier))
  cat(sprintf("  -> Skill:            %.2f%% improvement over baseline\n", brier_skill))

  results[[label]] <- list(
    wicket_rate = stats$wicket_rate,
    log_loss = stats$log_loss,
    baseline_logloss = baseline_logloss,
    logloss_skill = logloss_skill,
    brier_score = stats$brier_score,
    baseline_brier = baseline_brier,
    brier_skill = brier_skill
  )
}

# Run prediction analysis
cat("\n")
cli::cli_h1("Run Prediction Quality Analysis")
cat("\n")

for (i in seq_along(tables)) {
  tbl <- names(tables)[i]
  label <- tables[i]

  stats <- DBI::dbGetQuery(conn, sprintf("
    SELECT
      AVG(actual_runs) as mean_actual,
      AVG(exp_runs) as mean_pred,
      STDDEV(actual_runs) as sd_actual,
      STDDEV(exp_runs) as sd_pred,
      CORR(exp_runs, actual_runs) as correlation,
      AVG(ABS(exp_runs - actual_runs)) as mae,
      -- Poisson deviance (proper loss for count data)
      AVG(exp_runs - actual_runs * LN(GREATEST(exp_runs, 0.01))) as poisson_loss,
      -- Baseline loss (always predict mean)
      AVG(%f - actual_runs * LN(GREATEST(%f, 0.01))) as baseline_poisson
    FROM %s
  ",
  # Need to get mean first for baseline
  DBI::dbGetQuery(conn, sprintf("SELECT AVG(actual_runs) FROM %s", tbl))[[1]],
  DBI::dbGetQuery(conn, sprintf("SELECT AVG(actual_runs) FROM %s", tbl))[[1]],
  tbl))

  poisson_skill <- (1 - (stats$poisson_loss / stats$baseline_poisson)) * 100

  cat(sprintf("\n--- %s ---\n", label))
  cat(sprintf("Actual runs:     mean=%.3f, sd=%.3f\n", stats$mean_actual, stats$sd_actual))
  cat(sprintf("Predicted runs:  mean=%.3f, sd=%.3f\n", stats$mean_pred, stats$sd_pred))
  cat(sprintf("Correlation:     %.4f\n", stats$correlation))
  cat(sprintf("MAE:             %.4f runs\n", stats$mae))
  cat(sprintf("Poisson Loss:    %.4f (baseline: %.4f)\n", stats$poisson_loss, stats$baseline_poisson))
  cat(sprintf("  -> Skill:      %.2f%% improvement over baseline\n", poisson_skill))
}

# Venue ELO analysis
cat("\n")
cli::cli_h1("Venue ELO Investigation")
cat("\n")

for (i in seq_along(tables)) {
  tbl <- names(tables)[i]
  label <- tables[i]

  # Get venue statistics
  venue_stats <- DBI::dbGetQuery(conn, sprintf("
    WITH venue_latest AS (
      SELECT DISTINCT ON (venue)
        venue,
        venue_perm_run_elo_after as perm_elo,
        venue_perm_wicket_elo_after as perm_wicket_elo,
        venue_balls
      FROM %s
      ORDER BY venue, match_date DESC, delivery_id DESC
    )
    SELECT
      COUNT(*) as n_venues,
      AVG(perm_elo) as mean_elo,
      STDDEV(perm_elo) as sd_elo,
      MIN(perm_elo) as min_elo,
      MAX(perm_elo) as max_elo,
      PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY perm_elo) as p5,
      PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY perm_elo) as p95,
      AVG(venue_balls) as avg_balls
    FROM venue_latest
    WHERE venue_balls >= 500
  ", tbl))

  cat(sprintf("\n--- %s (venues with 500+ balls) ---\n", label))
  cat(sprintf("N venues:        %d\n", venue_stats$n_venues))
  cat(sprintf("Mean ELO:        %.1f (sd: %.1f)\n", venue_stats$mean_elo, venue_stats$sd_elo))
  cat(sprintf("Range:           %.0f to %.0f\n", venue_stats$min_elo, venue_stats$max_elo))
  cat(sprintf("5th-95th %%ile:   %.0f to %.0f\n", venue_stats$p5, venue_stats$p95))
  cat(sprintf("Avg balls/venue: %.0f\n", venue_stats$avg_balls))

  # Check if extreme venues have low sample sizes
  extremes <- DBI::dbGetQuery(conn, sprintf("
    WITH venue_latest AS (
      SELECT DISTINCT ON (venue)
        venue,
        venue_perm_run_elo_after as perm_elo,
        venue_balls
      FROM %s
      ORDER BY venue, match_date DESC, delivery_id DESC
    )
    SELECT venue, perm_elo, venue_balls
    FROM venue_latest
    WHERE perm_elo < (SELECT PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY perm_elo) FROM venue_latest WHERE venue_balls >= 500)
       OR perm_elo > (SELECT PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY perm_elo) FROM venue_latest WHERE venue_balls >= 500)
    ORDER BY perm_elo
    LIMIT 10
  ", tbl))

  if (nrow(extremes) > 0) {
    cat("\nExtreme venues (outside 5-95%ile):\n")
    for (j in 1:min(5, nrow(extremes))) {
      cat(sprintf("  %s: ELO=%.0f, balls=%d\n",
                  extremes$venue[j], extremes$perm_elo[j], extremes$venue_balls[j]))
    }
  }
}

cat("\n")
cli::cli_h1("Analysis Complete")
cat("\n")
