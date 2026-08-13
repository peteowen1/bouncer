# Forward-looking rating validation harness.
#
# Spec: .scratch/player-delivery-elo/issues/03-validation-harness.md (bouncerverse repo).
# Scores any player rating by whether it predicts that player's NEXT 12 MONTHS of output,
# against two tuned baselines. Deliberately not tied to the 3-Way ELO: pass any
# (player_id, match_date, rating) table.
#
# Design commitments that must not be quietly changed:
#   - Targets are per-ball rates (runs per ball, dismissals per ball), not averages.
#   - One player one vote above the exposure floors; ball-weighting is a secondary report.
#   - Both baselines have their hyperparameters tuned on prior origins under the same loss.
#   - Everything used to produce a rating or tune a parameter is strictly before the origin.

#' Default evaluation origins for the validation harness
#'
#' 1 January of each year from 2016 to 2025. 2016 is calibration-only (there is no
#' prior origin to fit on), so scored origins are 2017-2025.
#' @export
RATING_VAL_ORIGINS <- as.Date(paste0(2016:2025, "-01-01"))

#' Minimum career balls before the origin for a player to be evaluated
#' @export
RATING_VAL_MIN_CAREER_BALLS <- 500

#' Minimum balls inside the 12-month scoring window
#' @export
RATING_VAL_MIN_WINDOW_BALLS <- 200

RATING_VAL_K_GRID <- c(50, 100, 200, 400, 800, 1600, 3200)   # shrinkage, in balls
RATING_VAL_H_GRID <- c(180, 365, 730, 1095, 1825, 3650)      # half-life, in days

#' Load Per-Player Per-Match Exposure and Outcomes
#'
#' Aggregates the men's Test + first-class delivery pool to one row per
#' (player, match), with balls, runs and events (dismissals for batters,
#' bowler-credited wickets for bowlers).
#'
#' Definitional choices, per the ticket 03 spec:
#' \itemize{
#'   \item Batter balls faced exclude wides; batter runs are \code{runs_batter} only,
#'     so byes, leg byes and other extras are not credited to the batter.
#'   \item Bowler balls bowled exclude wides and no-balls; runs conceded include them.
#'   \item Bowler wickets exclude run outs, retirements, obstruction, handled ball
#'     and timed out.
#' }
#'
#' @param con A DBI connection to the bouncer DuckDB database.
#' @param role Either "batter" or "bowler".
#' @return A `data.table` with columns player_id, match_id, match_date, balls, runs, events.
#' @export
load_rating_pool <- function(con, role = c("batter", "bowler")) {
  role <- match.arg(role)
  base <- "FROM cricsheet.deliveries WHERE match_type IN ('Test','MDM') AND gender='male'"
  if (role == "batter") {
    ex <- DBI::dbGetQuery(con, paste0("
      SELECT batter_id AS player_id, match_id, match_date,
             COUNT(*) FILTER (WHERE COALESCE(wides,0)=0) AS balls,
             SUM(runs_batter) AS runs ", base, " GROUP BY 1,2,3"))
    ev <- DBI::dbGetQuery(con, paste0("
      SELECT player_out_id AS player_id, match_id, match_date, COUNT(*) AS events ",
      base, " AND is_wicket AND player_out_id IS NOT NULL GROUP BY 1,2,3"))
  } else {
    ex <- DBI::dbGetQuery(con, paste0("
      SELECT bowler_id AS player_id, match_id, match_date,
             COUNT(*) FILTER (WHERE COALESCE(wides,0)=0 AND COALESCE(noballs,0)=0) AS balls,
             SUM(runs_batter + COALESCE(wides,0) + COALESCE(noballs,0)) AS runs ",
      base, " GROUP BY 1,2,3"))
    ev <- DBI::dbGetQuery(con, paste0("
      SELECT bowler_id AS player_id, match_id, match_date, COUNT(*) AS events ", base,
      " AND is_wicket AND wicket_kind NOT IN ('run out','retired hurt','retired out',
        'obstructing the field','handled the ball','timed out') GROUP BY 1,2,3"))
  }
  ex <- data.table::as.data.table(ex)
  ev <- data.table::as.data.table(ev)
  d <- merge(ex, ev, by = c("player_id", "match_id", "match_date"), all.x = TRUE)
  d[is.na(events), events := 0L]
  d[, match_date := as.Date(match_date)]
  d[balls > 0][order(player_id, match_date)]
}

#' Build the (Origin, Player) Evaluation Frame
#'
#' For each origin, assembles each qualifying player's pre-origin career totals, their
#' recency-weighted totals across the half-life grid, their realised outcomes in the
#' following 12 months, and their rating as of the origin.
#'
#' @param pool Output of [load_rating_pool()].
#' @param ratings A table with player_id, match_date and one or more rating columns.
#'   The rating used at an origin is the last row strictly before that origin.
#' @param rating_cols Character vector of rating column names to carry through.
#' @param origins Evaluation origins. Defaults to [RATING_VAL_ORIGINS].
#' @return A `data.table`, one row per (origin, qualifying player).
#' @export
build_rating_frame <- function(pool, ratings, rating_cols,
                               origins = RATING_VAL_ORIGINS) {
  data.table::rbindlist(lapply(origins, function(T0) {
    win_end <- seq(T0, by = "1 year", length.out = 2)[2]
    hist <- pool[match_date < T0]
    win  <- pool[match_date >= T0 & match_date < win_end]
    h <- hist[, list(career_balls = sum(balls), career_runs = sum(runs),
                     career_events = sum(events)), by = player_id]
    w <- win[, list(win_balls = sum(balls), win_runs = sum(runs),
                    win_events = sum(events)), by = player_id]
    f <- merge(h, w, by = "player_id")
    f <- f[career_balls >= RATING_VAL_MIN_CAREER_BALLS &
             win_balls >= RATING_VAL_MIN_WINDOW_BALLS]
    if (!nrow(f)) return(NULL)
    for (h_days in RATING_VAL_H_GRID) {
      e <- hist[, {
        wt <- 2^(-as.numeric(T0 - match_date) / h_days)
        list(b = sum(balls * wt), r = sum(runs * wt), v = sum(events * wt))
      }, by = player_id]
      f <- merge(f, e, by = "player_id", all.x = TRUE)
      data.table::setnames(f, c("b", "r", "v"),
                           paste0(c("ew_b_", "ew_r_", "ew_v_"), h_days))
    }
    rt <- ratings[match_date < T0][, .SD[.N], by = player_id, .SDcols = rating_cols]
    f <- merge(f, rt, by = "player_id", all.x = TRUE)
    f[, origin := T0][]
  }))
}

# Poisson deviance for runs, binomial deviance for dismissals/wickets.
poisson_deviance <- function(y, mu) {
  mu <- pmax(mu, 1e-9)
  2 * (ifelse(y > 0, y * log(y / mu), 0) - (y - mu))
}
binomial_deviance <- function(y, n, p) {
  p <- pmin(pmax(p, 1e-9), 1 - 1e-9)
  2 * (ifelse(y > 0, y * log(y / (n * p)), 0) +
         ifelse(n - y > 0, (n - y) * log((n - y) / (n * (1 - p))), 0))
}
# Per player: deviance divided by that player's window balls, then averaged with
# EQUAL WEIGHT across players. This is the one-player-one-vote commitment.
loss_runs_rate <- function(f, rate) {
  mean(poisson_deviance(f$win_runs, rate * f$win_balls) / f$win_balls)
}
loss_event_rate <- function(f, rate) {
  mean(binomial_deviance(f$win_events, f$win_balls, rate) / f$win_balls)
}

#' Score a Rating Against Both Tuned Baselines
#'
#' Rolling-origin evaluation. At each scored origin, the baseline hyperparameters and the
#' rating-to-rate calibration are fitted on origins strictly earlier, so nothing leaks.
#'
#' @param frame Output of [build_rating_frame()].
#' @param target "runs" (runs per ball) or "events" (dismissals or wickets per ball).
#' @param rating_col Name of the rating column to score.
#' @param origins Origins to iterate. The first is calibration-only and is never scored.
#' @return A `data.table`, one row per scored origin, with the losses for the rating and
#'   both baselines, the selected hyperparameters, and Spearman correlations.
#' @export
score_rating <- function(frame, target = c("runs", "events"), rating_col,
                         origins = RATING_VAL_ORIGINS) {
  target <- match.arg(target)
  num_col <- if (target == "runs") "career_runs" else "career_events"
  ew_num  <- if (target == "runs") "ew_r_" else "ew_v_"
  lossf   <- if (target == "runs") loss_runs_rate else loss_event_rate
  cap     <- if (target == "runs") 5 else 0.5
  shrunk <- function(f, k) {
    (f[[num_col]] + k * (sum(f[[num_col]]) / sum(f$career_balls))) / (f$career_balls + k)
  }
  ewma <- function(f, h) {
    pmin(pmax((f[[paste0(ew_num, h)]] + 1e-6) / (f[[paste0("ew_b_", h)]] + 1e-6), 1e-6), cap)
  }
  res <- list()
  for (T0 in as.list(origins[-1])) {
    prior <- frame[origin < T0]
    cur   <- frame[origin == T0]
    if (!nrow(prior) || !nrow(cur)) next
    k <- RATING_VAL_K_GRID[which.min(vapply(RATING_VAL_K_GRID,
      function(kk) lossf(prior, shrunk(prior, kk)), numeric(1)))]
    h <- RATING_VAL_H_GRID[which.min(vapply(RATING_VAL_H_GRID,
      function(hh) lossf(prior, ewma(prior, hh)), numeric(1)))]
    pr <- prior[!is.na(get(rating_col))]
    cc <- cur[!is.na(get(rating_col))]
    pm <- NA_real_
    if (nrow(pr) > 30 && nrow(cc) > 0) {
      if (target == "runs") {
        # offset inside the formula and the rate read off the coefficients, so no
        # training offset can leak into predict() on new data
        fit <- stats::glm(win_runs ~ rt + offset(log(win_balls)), family = stats::poisson,
                          data = data.frame(win_runs = pr$win_runs,
                                            win_balls = pr$win_balls,
                                            rt = pr[[rating_col]]))
        cf <- stats::coef(fit)
        pm <- exp(cf[[1]] + cf[[2]] * cc[[rating_col]])
      } else {
        fit <- stats::glm(cbind(win_events, win_balls - win_events) ~ rt,
                          family = stats::binomial,
                          data = data.frame(win_events = pr$win_events,
                                            win_balls = pr$win_balls,
                                            rt = pr[[rating_col]]))
        pm <- stats::predict(fit, newdata = data.frame(rt = cc[[rating_col]]),
                             type = "response")
      }
    }
    realised <- function(d) {
      if (target == "runs") d$win_runs / d$win_balls else d$win_events / d$win_balls
    }
    res[[length(res) + 1]] <- data.table::data.table(
      origin = T0, n = nrow(cur), n_rated = nrow(cc), k = k, h = h,
      loss_b1 = lossf(cur, shrunk(cur, k)),
      loss_b2 = lossf(cur, ewma(cur, h)),
      loss_rating = if (length(pm) > 1) lossf(cc, pm) else NA_real_,
      rho_rating = if (nrow(cc) > 2)
        stats::cor(cc[[rating_col]], realised(cc), method = "spearman") else NA_real_,
      rho_b1 = stats::cor(shrunk(cur, k), realised(cur), method = "spearman"))
  }
  data.table::rbindlist(res)
}

#' Summarise a Scored Rating
#'
#' @param scored Output of [score_rating()].
#' @param label Human-readable name for the scored rating.
#' @return Invisibly, a one-row `data.table` of pooled results. Prints a summary.
#' @export
summarise_rating_score <- function(scored, label = "rating") {
  out <- data.table::data.table(
    label = label,
    loss_b1 = mean(scored$loss_b1),
    loss_b2 = mean(scored$loss_b2),
    loss_rating = mean(scored$loss_rating, na.rm = TRUE),
    rho_rating = mean(scored$rho_rating, na.rm = TRUE),
    rho_b1 = mean(scored$rho_b1, na.rm = TRUE))
  out[, skill_vs_career := 1 - loss_rating / loss_b1]
  out[, skill_vs_recency := 1 - loss_rating / loss_b2]
  cat(sprintf(
    "%-34s skill vs career %+7.1f%%  vs recency %+7.1f%%  rho %+.3f (baseline %+.3f)\n",
    label, 100 * out$skill_vs_career, 100 * out$skill_vs_recency,
    out$rho_rating, out$rho_b1))
  invisible(out)
}


#' Reliability of a Per-Observation Rating Input
#'
#' One-way random-effects decomposition (ICC(1)) of a per-innings quantity into
#' between-player signal and within-player noise, plus the Spearman-Brown
#' reliability of a player mean over `n` observations.
#'
#' @section Why this is here:
#' A leaderboard built from a noisy per-innings statistic orders sampling error,
#' not players, and nothing in the output looks wrong when it does. Measured on
#' `batting_era` (the term that dominates EPR) in 2026-08-13:
#'
#' | | within sd | between sd | reliability at n |
#' |---|---|---|---|
#' | T20 batting | 14.16 | 2.03 | 0.403 at n=33 |
#' | ODI batting | 27.86 | 4.93 | 0.467 at n=28 |
#'
#' Roughly half of the observed spread between players was sampling error, which
#' is why the T20 leaderboard put Shreyas Iyer first on a mean of 7.66 with a
#' median of -0.39. Run this before trusting, tuning against, or publishing any
#' ranking built from a per-innings value.
#'
#' @param value Numeric vector, one element per observation (e.g. per innings).
#' @param player Grouping vector of the same length identifying the player.
#' @param min_obs Integer. Players with fewer observations than this are dropped
#'   before the decomposition. Default 2, the minimum for a within-player
#'   variance to exist at all.
#'
#' @return A list with `within_sd`, `between_sd` (sampling-error corrected),
#'   `icc` (single-observation reliability), `n_players`, `n_obs`,
#'   `mean_obs_per_player`, `reliability` (of a mean over
#'   `mean_obs_per_player`), and `obs_for(target)`, the number of observations
#'   needed to reach a given reliability. `between_sd` is floored at zero: a
#'   negative variance estimate means the data cannot distinguish players at all.
#'
#' @export
rating_reliability <- function(value, player, min_obs = 2L) {

  if (length(value) != length(player)) {
    cli::cli_abort("{.arg value} and {.arg player} must be the same length.")
  }

  keep <- !is.na(value) & !is.na(player)
  value <- value[keep]
  player <- as.character(player[keep])

  counts <- table(player)
  eligible <- names(counts)[counts >= min_obs]
  if (length(eligible) < 2L) {
    cli::cli_abort("Need at least 2 players with {min_obs}+ observations; found {length(eligible)}.")
  }
  sel <- player %in% eligible
  value <- value[sel]
  player <- player[sel]

  k <- length(eligible)
  N <- length(value)
  n_i <- as.numeric(table(player)[eligible])
  means <- vapply(split(value, player), mean, numeric(1))[eligible]
  grand <- mean(value)

  # Standard one-way ANOVA, unbalanced. n0 is the effective group size; using
  # mean(n_i) instead biases the between-player variance when sizes differ.
  MSB <- sum(n_i * (means - grand)^2) / (k - 1)
  MSW <- sum((value - means[player])^2) / (N - k)
  n0  <- (N - sum(n_i^2) / N) / (k - 1)

  s2_between <- max(0, (MSB - MSW) / n0)
  icc <- if (s2_between + MSW > 0) s2_between / (s2_between + MSW) else 0
  nbar <- N / k

  list(
    within_sd  = sqrt(MSW),
    between_sd = sqrt(s2_between),
    icc = icc,
    n_players = k,
    n_obs = N,
    mean_obs_per_player = nbar,
    reliability = if (icc > 0) nbar * icc / (1 + (nbar - 1) * icc) else 0,
    obs_for = function(target) {
      if (icc <= 0 || target >= 1) return(Inf)
      target * (1 - icc) / (icc * (1 - target))
    }
  )
}
