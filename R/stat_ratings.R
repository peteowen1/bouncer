# Player Stat Rating Estimation
# ==============================
# Bayesian conjugate prior estimation of per-stat player ratings,
# with exponential time decay and role-specific priors.
#
# Rate stats use Gamma-Poisson (counts per ball faced/bowled).
# Efficiency stats use Beta-Binomial (proportions).
#
# Adapted from torpverse/torp/R/player_skills.R for cricket.
# Key difference: exposure is balls (faced/bowled) not TOG.


#' Estimate Player Stat Ratings Using Bayesian Conjugate Priors
#'
#' For each player, estimates "true stat rating" at a reference date using
#' all prior matches. Uses conjugate Bayesian updating with exponential
#' time decay.
#'
#' \strong{Rate stats (per-ball):} Gamma-Poisson model. Raw event counts
#' are decay-weighted; the Gamma prior is centered on the role mean with
#' strength controlled by \code{prior_strength}. Posterior mean gives the
#' estimated per-ball rate.
#'
#' \strong{Efficiency stats (proportions):} Beta-Binomial model. Successes
#' and attempts are decay-weighted; the Beta prior is centered on the
#' role mean with strength controlled by \code{prior_strength}.
#'
#' @param stat_data data.table from \code{\link{prepare_stat_rating_data}}.
#' @param ref_date Date to estimate ratings as of. Only matches before this
#'   date are used. If NULL, uses one day after the latest match.
#' @param params Named list from \code{\link{default_stat_rating_params}}.
#' @param stat_defs data.frame from \code{\link{stat_rating_definitions}}.
#' @param compute_ci Logical. If TRUE (default), compute credible intervals.
#'
#' @return data.table with one row per player containing:
#'   \code{player_id}, \code{role_group}, \code{n_matches}, \code{wt_matches},
#'   \code{ref_date}, and for each stat: \code{{stat}_rating},
#'   \code{{stat}_rating_lower}, \code{{stat}_rating_upper}.
#'
#' @importFrom data.table as.data.table copy
#' @importFrom stats qgamma qbeta
#' @export
estimate_player_stat_ratings <- function(stat_data, ref_date = NULL,
                                         params = NULL, stat_defs = NULL,
                                         compute_ci = TRUE) {
  if (is.null(params)) params <- default_stat_rating_params()
  if (is.null(stat_defs)) stat_defs <- stat_rating_definitions()

  dt <- data.table::as.data.table(stat_data)

  if (!inherits(dt$match_date_rating, "Date")) {
    dt[, match_date_rating := as.Date(match_date_rating)]
  }

  if (is.null(ref_date)) {
    ref_date <- max(dt$match_date_rating, na.rm = TRUE) + 1L
  }
  ref_date <- as.Date(ref_date)

  dt <- dt[match_date_rating < ref_date]
  if (nrow(dt) == 0) {
    cli::cli_warn("No match data available before ref_date.")
    return(data.table::data.table())
  }

  dt[, days_since := as.numeric(ref_date - match_date_rating)]

  # Player metadata and match counts
  player_meta <- dt[, .(
    role_group = {
      rg <- role_group[!is.na(role_group)]
      if (length(rg) == 0) "BATTER"
      else { tt <- table(rg); names(tt)[which.max(tt)] }
    }
  ), by = player_id]

  w_rate_vec <- exp(-params$lambda_rate * dt$days_since)
  match_counts <- dt[, {
    first <- !duplicated(match_id)
    .(n_matches = sum(first),
      wt_matches = sum(w_rate_vec[.I] * first, na.rm = TRUE))
  }, by = player_id]

  ci_alpha <- (1 - params$credible_level) / 2

  rate_defs <- stat_defs[stat_defs$type == "rate", ]
  eff_defs <- stat_defs[stat_defs$type == "efficiency", ]

  stat_results <- list()
  skipped_stats <- character(0)

  stat_params <- params$stat_params
  role_groups <- names(stat_rating_role_map())

  # -------------------------------------------------------
  # Rate stats (Gamma-Poisson)
  # -------------------------------------------------------
  for (i in seq_len(nrow(rate_defs))) {
    stat_nm <- rate_defs$stat_name[i]
    src_col <- rate_defs$source_col[i]
    exp_col <- rate_defs$exposure_col[i]

    if (!src_col %in% names(dt) || !exp_col %in% names(dt)) {
      skipped_stats <- c(skipped_stats, stat_nm)
      next
    }

    # Per-stat hyperparameters
    sp <- if (stat_nm %in% names(stat_params)) stat_params[[stat_nm]]
          else list(lambda = params$lambda_rate, prior_strength = params$prior_games_rate)
    stat_lambda <- sp$lambda
    prior_str <- sp$prior_strength

    vals <- as.numeric(dt[[src_col]])
    vals[is.na(vals)] <- 0
    exposure <- as.numeric(dt[[exp_col]])
    exposure[is.na(exposure)] <- 0

    w_vec <- exp(-stat_lambda * dt$days_since)

    # Aggregate per player
    data.table::set(dt, j = ".wnum", value = w_vec * vals)
    data.table::set(dt, j = ".wden", value = w_vec * exposure)

    agg <- dt[, .(w_num = sum(.wnum, na.rm = TRUE),
                   w_den = sum(.wden, na.rm = TRUE)), by = player_id]

    agg[player_meta, role_group := i.role_group, on = "player_id"]

    # Role-specific prior means
    total_exposure <- sum(w_vec * exposure, na.rm = TRUE)
    grand_mean <- if (total_exposure > 0) sum(w_vec * vals, na.rm = TRUE) / total_exposure else 0

    role_means <- stats::setNames(rep(grand_mean, length(role_groups)), role_groups)
    role_means_dt <- dt[!is.na(role_group), {
      wd <- sum(.wden, na.rm = TRUE)
      .(role_mean = if (wd > 0) sum(.wnum, na.rm = TRUE) / wd else grand_mean)
    }, by = role_group]
    role_means[role_means_dt$role_group] <- role_means_dt$role_mean

    # Posterior
    agg[, alpha0 := {
      mu <- role_means[role_group]
      mu[is.na(mu)] <- grand_mean
      mu * prior_str
    }]
    agg[, `:=`(
      alpha_post = alpha0 + w_num,
      beta_post = prior_str + w_den
    )]
    # Clamp alpha_post to small positive value — WPA/ERA stats can produce
    # negative w_num, which would make qgamma() return NaN
    agg[alpha_post <= 0, alpha_post := 1e-4]

    rating_col <- paste0(stat_nm, "_rating")
    lower_col <- paste0(stat_nm, "_rating_lower")
    upper_col <- paste0(stat_nm, "_rating_upper")

    agg[, (rating_col) := alpha_post / beta_post]
    if (compute_ci) {
      agg[, (lower_col) := stats::qgamma(ci_alpha, shape = alpha_post, rate = beta_post)]
      agg[, (upper_col) := stats::qgamma(1 - ci_alpha, shape = alpha_post, rate = beta_post)]
    }

    keep_cols <- c("player_id", rating_col)
    if (compute_ci) keep_cols <- c(keep_cols, lower_col, upper_col)
    stat_results[[stat_nm]] <- agg[, keep_cols, with = FALSE]
  }

  # -------------------------------------------------------
  # Efficiency stats (Beta-Binomial)
  # -------------------------------------------------------
  for (i in seq_len(nrow(eff_defs))) {
    stat_nm <- eff_defs$stat_name[i]
    success_col <- eff_defs$success_col[i]
    attempts_col <- eff_defs$attempts_col[i]

    if (is.na(success_col) || is.na(attempts_col)) next
    if (!success_col %in% names(dt) || !attempts_col %in% names(dt)) {
      skipped_stats <- c(skipped_stats, stat_nm)
      next
    }

    sp <- if (stat_nm %in% names(stat_params)) stat_params[[stat_nm]]
          else list(lambda = params$lambda_efficiency, prior_strength = params$prior_attempts_efficiency)
    stat_lambda <- sp$lambda
    prior_str <- sp$prior_strength

    successes <- as.numeric(dt[[success_col]])
    successes[is.na(successes)] <- 0
    attempts <- as.numeric(dt[[attempts_col]])
    attempts[is.na(attempts)] <- 0
    successes <- pmin(successes, attempts)

    w_vec <- exp(-stat_lambda * dt$days_since)

    data.table::set(dt, j = ".wnum", value = w_vec * successes)
    data.table::set(dt, j = ".wden", value = w_vec * attempts)

    agg <- dt[, .(w_num = sum(.wnum, na.rm = TRUE),
                   w_den = sum(.wden, na.rm = TRUE)), by = player_id]

    agg[player_meta, role_group := i.role_group, on = "player_id"]

    # Grand mean proportion
    total_att <- sum(w_vec * attempts, na.rm = TRUE)
    grand_prop <- if (total_att > 0) sum(w_vec * successes, na.rm = TRUE) / total_att else 0.5
    grand_prop <- max(min(grand_prop, 1 - 1e-6), 1e-6)

    # Role-specific proportions
    role_props <- stats::setNames(rep(grand_prop, length(role_groups)), role_groups)
    role_props_dt <- dt[!is.na(role_group), {
      pa <- sum(.wden, na.rm = TRUE)
      .(pp = if (pa > 0) max(min(sum(.wnum, na.rm = TRUE) / pa, 1 - 1e-6), 1e-6)
             else grand_prop)
    }, by = role_group]
    role_props[role_props_dt$role_group] <- role_props_dt$pp

    # Posterior
    agg[, mu0 := {
      m <- role_props[role_group]
      m[is.na(m)] <- grand_prop
      pmax(pmin(m, 1 - 1e-6), 1e-6)
    }]
    agg[, `:=`(
      alpha_post = mu0 * prior_str + w_num,
      beta_post = (1 - mu0) * prior_str + w_den - w_num
    )]

    # Clamp invalid Beta parameters
    agg[alpha_post <= 0, alpha_post := 1e-4]
    agg[beta_post <= 0, beta_post := 1e-4]

    rating_col <- paste0(stat_nm, "_rating")
    lower_col <- paste0(stat_nm, "_rating_lower")
    upper_col <- paste0(stat_nm, "_rating_upper")

    agg[, (rating_col) := alpha_post / (alpha_post + beta_post)]
    if (compute_ci) {
      agg[, (lower_col) := stats::qbeta(ci_alpha, alpha_post, beta_post)]
      agg[, (upper_col) := stats::qbeta(1 - ci_alpha, alpha_post, beta_post)]
    }

    keep_cols <- c("player_id", rating_col)
    if (compute_ci) keep_cols <- c(keep_cols, lower_col, upper_col)
    stat_results[[stat_nm]] <- agg[, keep_cols, with = FALSE]
  }

  if (length(skipped_stats) > 0) {
    cli::cli_warn("Skipped {length(skipped_stats)} stat(s): {paste(skipped_stats, collapse = ', ')}")
  }

  # Clean up temp columns
  tmp_cols <- intersect(c(".wnum", ".wden"), names(dt))
  for (tc in tmp_cols) data.table::set(dt, j = tc, value = NULL)

  # Assemble result
  result <- data.table::copy(player_meta)
  result[match_counts, `:=`(n_matches = i.n_matches, wt_matches = i.wt_matches), on = "player_id"]
  result[, ref_date := ref_date]

  for (stat_nm in names(stat_results)) {
    sr <- stat_results[[stat_nm]]
    idx <- match(result$player_id, sr$player_id)
    cols <- setdiff(names(sr), "player_id")
    for (col in cols) data.table::set(result, j = col, value = sr[[col]][idx])
  }

  # Filter to minimum weighted matches
  min_wt <- params$min_wt_matches %||% 3
  result <- result[wt_matches >= min_wt]
  data.table::setorder(result, -wt_matches)

  cli::cli_alert_success("Estimated ratings for {nrow(result)} players ({length(stat_results)} stats)")
  result
}


#' Store Stat Ratings to DuckDB
#'
#' @param conn DBI connection (writable).
#' @param ratings data.table from \code{\link{estimate_player_stat_ratings}}.
#' @param format Character. Match format.
#'
#' @return Number of rows stored (invisibly).
#' @keywords internal
store_stat_ratings <- function(conn, ratings, format = c("t20", "odi", "test")) {
  format <- match.arg(format)
  table_name <- paste0(format, "_stat_ratings")

  if (nrow(ratings) == 0) {
    cli::cli_alert_warning("No ratings to store for {toupper(format)}")
    return(invisible(0L))
  }

  # Create table if not exists (dynamic schema based on rating columns)
  if (!table_name %in% DBI::dbListTables(conn)) {
    duckdb::duckdb_register(conn, "sr_staging", ratings)
    DBI::dbExecute(conn, sprintf(
      "CREATE TABLE %s AS SELECT * FROM sr_staging WHERE 1=0", table_name
    ))
    duckdb::duckdb_unregister(conn, "sr_staging")
  }

  # Replace all existing data (stat ratings are a point-in-time snapshot)
  DBI::dbExecute(conn, sprintf("DELETE FROM %s", table_name))

  duckdb::duckdb_register(conn, "sr_staging", ratings)
  on.exit(duckdb::duckdb_unregister(conn, "sr_staging"), add = TRUE)

  n <- DBI::dbExecute(conn, sprintf("INSERT INTO %s SELECT * FROM sr_staging", table_name))
  cli::cli_alert_success("Stored {n} player ratings in {table_name}")
  invisible(n)
}


#' Load Stat Ratings
#'
#' @param format Character. Match format.
#' @param source Character. "local" or "remote".
#' @param path Character. Custom DB path (local only).
#'
#' @return data.table with player stat ratings.
#' @export
load_stat_ratings <- function(format = c("t20", "odi", "test"),
                               source = c("local", "remote"),
                               path = NULL) {
  format <- match.arg(format)
  source <- match.arg(source)

  if (source == "remote") {
    file_name <- sprintf("%s_stat_ratings.parquet", format)
    return(load_release_parquet("ratings", file_name))
  }

  conn <- get_db_connection(path = path, read_only = TRUE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  table_name <- paste0(format, "_stat_ratings")
  if (!table_name %in% DBI::dbListTables(conn)) {
    cli::cli_abort("Table {table_name} not found.")
  }

  result <- DBI::dbGetQuery(conn, sprintf("SELECT * FROM %s ORDER BY wt_matches DESC", table_name))
  data.table::as.data.table(result)
}
