# Optimize Stat Rating Hyperparameters ----
#
# Finds optimal (lambda, prior_strength) per stat using next-match prediction.
# Rate stats: minimize exposure-weighted MSE
# Efficiency stats: minimize attempt-weighted log-loss
#
# Uses BOBYQA (derivative-free) with multi-start grid search.
# Adapted from torpverse/torp/data-raw/06-stat-ratings/02_optimize_stat_rating_params.R

library(cli)
library(data.table)
devtools::load_all()

if (!requireNamespace("nloptr", quietly = TRUE)) {
  stop("Package 'nloptr' required for optimization. Install with: install.packages('nloptr')")
}

# ============================================================================
# Configuration
# ============================================================================

FORMAT <- "t20"  # Optimize on T20 (largest dataset), apply to all
MIN_PLAYER_GAMES <- 5
SAMPLE_N <- 500   # Max players for speed
SEED <- 42
TOP_N <- 5         # Multi-start: polish top-N starts
MAXEVAL <- 2000
XTOL_REL <- 1e-10

# Bounds
RATE_LOWER <- c(0.0001, 0.01)
RATE_UPPER <- c(0.04, 100)
EFF_LOWER <- c(0.00001, 0.1)
EFF_UPPER <- c(0.05, 500)

# ============================================================================
# Load and prepare data
# ============================================================================

cli::cli_h1("Loading data for {toupper(FORMAT)}")

pgd <- load_player_game_data(FORMAT, source = "local")
stat_data <- prepare_stat_rating_data(pgd)
stat_defs <- stat_rating_definitions()

# Sort chronologically
setorder(stat_data, player_id, match_date_rating)

# Filter to players with enough games
player_games <- stat_data[, .(n = .N), by = player_id]
eligible <- player_games[n >= MIN_PLAYER_GAMES]$player_id
cli::cli_alert_info("{length(eligible)} players with >= {MIN_PLAYER_GAMES} games")

set.seed(SEED)
if (length(eligible) > SAMPLE_N) {
  sampled <- sample(eligible, SAMPLE_N)
} else {
  sampled <- eligible
}

dt <- stat_data[player_id %in% sampled]
cli::cli_alert_info("Sampled {length(sampled)} players, {nrow(dt)} rows")

# Pre-compute group structure for cumsum trick
setorder(dt, player_id, match_date_rating)
dt[, row_idx := .I]
dt[, grp := .GRP, by = player_id]

group_start <- dt[, min(row_idx), by = grp]$V1
all_group <- dt$grp

# Days relative (for exp decay) — use earliest date as reference
ref_min <- min(dt$match_date_rating)
dt[, d_rel := as.numeric(match_date_rating - ref_min)]
all_d_rel <- dt$d_rel

# Prediction indices: predict game t from games 1..t-1
# pred_idx = index of game to predict, prev_idx = index of previous game (t-1)
dt[, game_num := seq_len(.N), by = player_id]
pred_mask <- dt$game_num >= 2  # Can only predict from game 2 onwards
pred_idx <- which(pred_mask)
prev_idx <- pred_idx - 1L  # Previous row (same player since sorted by player+date)


# ============================================================================
# Grouped cumsum (O(n) vectorized trick)
# ============================================================================

grouped_cumsum <- function(x, grp, grp_start) {
  cx <- cumsum(x)
  offsets <- c(0, cx[grp_start[-1] - 1L])
  cx - offsets[grp]
}


# ============================================================================
# Multi-start optimizer
# ============================================================================

multi_start_optim <- function(fn, starts, lower, upper, top_n = TOP_N) {
  # Phase 1: cheap grid eval
  grid_vals <- vapply(starts, fn, numeric(1))

  # Phase 2: polish top_n
  order_idx <- order(grid_vals)
  best_par <- starts[[order_idx[1]]]
  best_val <- grid_vals[order_idx[1]]

  n_polish <- min(top_n, length(starts))
  for (i in seq_len(n_polish)) {
    idx <- order_idx[i]
    result <- nloptr::bobyqa(
      x0 = starts[[idx]], fn = fn,
      lower = lower, upper = upper,
      control = list(maxeval = MAXEVAL, xtol_rel = XTOL_REL)
    )
    if (result$value < best_val) {
      best_val <- result$value
      best_par <- result$par
    }
  }

  list(par = best_par, value = best_val)
}


# ============================================================================
# Rate stat optimization
# ============================================================================

rate_starts <- list()
for (lam in c(0.001, 0.003, 0.005, 0.01, 0.02)) {
  for (ps in c(0.5, 1, 3, 5, 10, 20)) {
    rate_starts <- c(rate_starts, list(c(lam, ps)))
  }
}

rate_defs <- stat_defs[stat_defs$type == "rate", ]
rate_results <- list()

cli::cli_h1("Optimizing {nrow(rate_defs)} rate stats")

for (i in seq_len(nrow(rate_defs))) {
  stat_nm <- rate_defs$stat_name[i]
  src_col <- rate_defs$source_col[i]
  exp_col <- rate_defs$exposure_col[i]

  if (!src_col %in% names(dt) || !exp_col %in% names(dt)) {
    cli::cli_alert_warning("Skipping {stat_nm}: missing columns")
    next
  }

  stat_events <- as.numeric(dt[[src_col]])
  stat_events[is.na(stat_events)] <- 0
  stat_exposure <- as.numeric(dt[[exp_col]])
  stat_exposure[is.na(stat_exposure)] <- 0

  # Grand mean rate
  total_ev <- sum(stat_events, na.rm = TRUE)
  total_ex <- sum(stat_exposure, na.rm = TRUE)
  mu0 <- if (total_ex > 0) total_ev / total_ex else 0

  # Prediction targets
  pred_actual <- stat_events[pred_idx] / pmax(stat_exposure[pred_idx], 0.1)
  pred_wt <- stat_exposure[pred_idx]
  ok <- pred_wt > 0
  total_wt <- sum(pred_wt[ok])

  if (total_wt == 0) {
    cli::cli_alert_warning("Skipping {stat_nm}: no prediction data")
    next
  }

  fn <- function(par) {
    lambda <- par[1]
    prior_strength <- par[2]

    exp_pos <- exp(pmin(lambda * all_d_rel, 500))
    exp_neg <- exp(pmax(-lambda * all_d_rel, -500))

    cum_ev <- grouped_cumsum(exp_pos * stat_events, all_group, group_start)
    cum_ex <- grouped_cumsum(exp_pos * stat_exposure, all_group, group_start)

    w_ev <- exp_neg[pred_idx] * cum_ev[prev_idx]
    w_ex <- exp_neg[pred_idx] * cum_ex[prev_idx]

    predicted <- (mu0 * prior_strength + w_ev) / (prior_strength + w_ex)

    loss <- sum(pred_wt[ok] * (predicted[ok] - pred_actual[ok])^2) / total_wt
    if (is.nan(loss) || is.infinite(loss)) return(1e6)
    loss
  }

  result <- multi_start_optim(fn, rate_starts, RATE_LOWER, RATE_UPPER)
  rate_results[[stat_nm]] <- list(
    lambda = result$par[1],
    prior_strength = result$par[2],
    loss = result$value
  )

  cli::cli_alert_success("{stat_nm}: lambda={round(result$par[1], 5)}, prior={round(result$par[2], 2)}, MSE={round(result$value, 6)}")
}


# ============================================================================
# Efficiency stat optimization
# ============================================================================

eff_starts <- list()
for (lam in c(0.0001, 0.001, 0.003, 0.01, 0.02)) {
  for (ps in c(1, 5, 20, 60, 100, 200)) {
    eff_starts <- c(eff_starts, list(c(lam, ps)))
  }
}

eff_defs <- stat_defs[stat_defs$type == "efficiency", ]
eff_results <- list()

cli::cli_h1("Optimizing {nrow(eff_defs)} efficiency stats")

for (i in seq_len(nrow(eff_defs))) {
  stat_nm <- eff_defs$stat_name[i]
  success_col <- eff_defs$success_col[i]
  attempts_col <- eff_defs$attempts_col[i]

  if (is.na(success_col) || is.na(attempts_col)) next
  if (!success_col %in% names(dt) || !attempts_col %in% names(dt)) {
    cli::cli_alert_warning("Skipping {stat_nm}: missing columns")
    next
  }

  stat_succ <- as.numeric(dt[[success_col]])
  stat_succ[is.na(stat_succ)] <- 0
  stat_att <- as.numeric(dt[[attempts_col]])
  stat_att[is.na(stat_att)] <- 0
  stat_succ <- pmin(stat_succ, stat_att)

  total_s <- sum(stat_succ, na.rm = TRUE)
  total_a <- sum(stat_att, na.rm = TRUE)
  mu0 <- if (total_a > 0) max(min(total_s / total_a, 1 - 1e-6), 1e-6) else 0.5

  pred_succ <- stat_succ[pred_idx]
  pred_att <- stat_att[pred_idx]
  eff_actual <- pred_succ / pmax(pred_att, 1)
  eff_actual <- pmax(pmin(eff_actual, 1 - 1e-8), 1e-8)
  ok <- pred_att > 0
  eff_total_wt <- sum(pred_att[ok])

  if (eff_total_wt == 0) {
    cli::cli_alert_warning("Skipping {stat_nm}: no prediction data")
    next
  }

  fn <- function(par) {
    lambda <- par[1]
    prior_strength <- par[2]

    alpha0 <- mu0 * prior_strength
    beta0 <- (1 - mu0) * prior_strength

    exp_pos <- exp(pmin(lambda * all_d_rel, 500))
    exp_neg <- exp(pmax(-lambda * all_d_rel, -500))

    cum_s <- grouped_cumsum(exp_pos * stat_succ, all_group, group_start)
    cum_a <- grouped_cumsum(exp_pos * stat_att, all_group, group_start)

    w_s <- exp_neg[pred_idx] * cum_s[prev_idx]
    w_a <- exp_neg[pred_idx] * cum_a[prev_idx]

    predicted <- (alpha0 + w_s) / (alpha0 + beta0 + w_a)
    predicted <- pmax(pmin(predicted, 1 - 1e-8), 1e-8)

    loss_i <- -(eff_actual * log(predicted) + (1 - eff_actual) * log(1 - predicted))
    loss <- sum(pred_att[ok] * loss_i[ok]) / eff_total_wt
    if (is.nan(loss) || is.infinite(loss)) return(1e6)
    loss
  }

  result <- multi_start_optim(fn, eff_starts, EFF_LOWER, EFF_UPPER)
  eff_results[[stat_nm]] <- list(
    lambda = result$par[1],
    prior_strength = result$par[2],
    loss = result$value
  )

  cli::cli_alert_success("{stat_nm}: lambda={round(result$par[1], 5)}, prior={round(result$par[2], 2)}, logloss={round(result$value, 6)}")
}


# ============================================================================
# Write results back to R/stat_rating_config.R
# ============================================================================

cli::cli_h1("Writing optimized parameters to R/stat_rating_config.R")

config_path <- file.path(getwd(), "R", "stat_rating_config.R")
config_lines <- readLines(config_path)

# Find .stat_rating_params function
fn_start <- grep("^\\.stat_rating_params <- function", config_lines)
if (length(fn_start) == 0) {
  cli::cli_abort("Could not find .stat_rating_params function in config file")
}
fn_start <- fn_start[1]

# Find closing brace by tracking depth
depth <- 0
fn_end <- fn_start
for (line_i in fn_start:length(config_lines)) {
  depth <- depth + nchar(gsub("[^{]", "", config_lines[line_i])) -
                   nchar(gsub("[^}]", "", config_lines[line_i]))
  if (depth == 0 && line_i > fn_start) {
    fn_end <- line_i
    break
  }
}

# Build new function body
all_results <- c(rate_results, eff_results)
max_name_len <- max(nchar(names(all_results)))

new_lines <- c(
  ".stat_rating_params <- function() {",
  "  list(",
  "    # Rate stats (Gamma-Poisson, optimized via multi-start MSE)"
)

rate_names <- names(rate_results)
for (j in seq_along(rate_names)) {
  nm <- rate_names[j]
  r <- rate_results[[nm]]
  padded <- formatC(nm, width = -max_name_len, flag = "-")
  comma <- if (j < length(rate_names) || length(eff_results) > 0) "," else ""
  lam_str <- if (r$lambda < 0.0001) formatC(r$lambda, format = "e", digits = 0) else sprintf("%.5f", r$lambda)
  new_lines <- c(new_lines, sprintf("    %s = list(lambda = %s, prior_strength = %.2f)%s",
                                     padded, lam_str, r$prior_strength, comma))
}

if (length(eff_results) > 0) {
  new_lines <- c(new_lines, "    # Efficiency stats (Beta-Binomial, optimized via multi-start log-loss)")
  eff_names <- names(eff_results)
  for (j in seq_along(eff_names)) {
    nm <- eff_names[j]
    r <- eff_results[[nm]]
    padded <- formatC(nm, width = -max_name_len, flag = "-")
    comma <- if (j < length(eff_names)) "," else ""
    lam_str <- if (r$lambda < 0.0001) formatC(r$lambda, format = "e", digits = 0) else sprintf("%.5f", r$lambda)
    new_lines <- c(new_lines, sprintf("    %s = list(lambda = %s, prior_strength = %.2f)%s",
                                       padded, lam_str, r$prior_strength, comma))
  }
}

new_lines <- c(new_lines, "  )", "}")

# Replace function in config
config_lines <- c(config_lines[1:(fn_start - 1)], new_lines, config_lines[(fn_end + 1):length(config_lines)])
writeLines(config_lines, config_path)

cli::cli_alert_success("Wrote {length(all_results)} optimized params to {config_path}")
cli::cli_h1("Optimization Complete")
