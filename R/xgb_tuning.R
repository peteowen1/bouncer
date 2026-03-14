# XGBoost Hyperparameter Tuning
#
# Random search tuning for XGBoost models with grouped CV folds.
# Uses random search (no extra dependencies) with match-grouped cross-validation.


#' Tune XGBoost Hyperparameters via Random Search
#'
#' Performs random search over XGBoost hyperparameters using match-grouped
#' cross-validation folds. Returns the best parameter set found.
#'
#' @param dtrain xgb.DMatrix. Training data
#' @param folds List of integer vectors. Grouped CV fold indices
#'   (e.g., from match-grouped splitting)
#' @param fixed_params List. Fixed XGBoost parameters (objective, eval_metric, num_class)
#' @param n_iter Integer. Number of random parameter combinations to try (default 20)
#' @param max_rounds Integer. Maximum boosting rounds per trial (default 2000)
#' @param early_stopping Integer. Early stopping patience (default 20)
#' @param seed Integer. Random seed for reproducibility
#' @param param_grid Named list. Parameter search spaces. Each element is a
#'   list with 'min' and 'max' for continuous params or a vector for discrete.
#'   Defaults to a sensible grid for tree-based models.
#' @param verbose Logical. Print progress (default TRUE)
#'
#' @return Named list with:
#'   - best_params: the best parameter combination found
#'   - best_score: the best CV score
#'   - best_nrounds: optimal number of rounds for best params
#'   - all_results: data.frame of all trial results
#'   - tuning_time_mins: total tuning duration in minutes
#'
#' @export
tune_xgb_params <- function(dtrain,
                            folds,
                            fixed_params = list(
                              objective = "multi:softprob",
                              num_class = 7,
                              eval_metric = "mlogloss"
                            ),
                            n_iter = 20,
                            max_rounds = 2000,
                            early_stopping = 20,
                            seed = 42,
                            param_grid = NULL,
                            verbose = TRUE) {

  if (!requireNamespace("xgboost", quietly = TRUE)) {
    stop("xgboost package required for tuning")
  }

  # Default parameter search space
  if (is.null(param_grid)) {
    param_grid <- list(
      max_depth = list(min = 4, max = 8, type = "integer"),
      eta = list(min = 0.01, max = 0.3, type = "log"),
      subsample = list(min = 0.6, max = 1.0, type = "continuous"),
      colsample_bytree = list(min = 0.5, max = 1.0, type = "continuous"),
      min_child_weight = list(min = 1, max = 10, type = "integer"),
      gamma = list(min = 0, max = 5, type = "continuous"),
      lambda = list(min = 0.5, max = 5, type = "continuous")
    )
  }

  start_time <- Sys.time()
  set.seed(seed)

  # Generate random parameter combinations
  trials <- vector("list", n_iter)
  for (i in seq_len(n_iter)) {
    params <- list()
    for (p_name in names(param_grid)) {
      spec <- param_grid[[p_name]]
      p_type <- spec$type %||% "continuous"

      if (p_type == "integer") {
        params[[p_name]] <- sample(spec$min:spec$max, 1)
      } else if (p_type == "log") {
        params[[p_name]] <- exp(runif(1, log(spec$min), log(spec$max)))
      } else {
        params[[p_name]] <- runif(1, spec$min, spec$max)
      }
    }
    trials[[i]] <- params
  }

  # Evaluate each combination
  results <- data.frame(
    trial = integer(n_iter),
    score = numeric(n_iter),
    best_nrounds = integer(n_iter),
    stringsAsFactors = FALSE
  )

  # Add param columns
  for (p_name in names(param_grid)) {
    results[[p_name]] <- numeric(n_iter)
  }

  best_score <- Inf
  best_params <- NULL
  best_nrounds <- 100

  for (i in seq_len(n_iter)) {
    trial_params <- c(fixed_params, trials[[i]])

    tryCatch({
      cv_result <- xgboost::xgb.cv(
        params = trial_params,
        data = dtrain,
        nrounds = max_rounds,
        folds = folds,
        early_stopping_rounds = early_stopping,
        verbose = 0
      )

      # Get best iteration (handle xgboost version differences)
      nrounds <- cv_result$early_stop$best_iteration %||%
        cv_result$best_iteration %||%
        cv_result$best_iter %||%
        cv_result$niter

      if (is.null(nrounds) || is.na(nrounds) || nrounds < 1) {
        eval_log <- cv_result$evaluation_log
        metric_col <- grep("^test_.*_mean$", names(eval_log), value = TRUE)[1]
        if (!is.null(metric_col)) {
          nrounds <- which.min(eval_log[[metric_col]])
        } else {
          nrounds <- 100
        }
      }

      # Get score at best iteration
      eval_log <- cv_result$evaluation_log
      metric_col <- grep("^test_.*_mean$", names(eval_log), value = TRUE)[1]
      score <- eval_log[[metric_col]][nrounds]

      results$trial[i] <- i
      results$score[i] <- score
      results$best_nrounds[i] <- nrounds

      for (p_name in names(param_grid)) {
        results[[p_name]][i] <- trials[[i]][[p_name]]
      }

      if (score < best_score) {
        best_score <- score
        best_params <- trials[[i]]
        best_nrounds <- nrounds
      }

      if (verbose) {
        cat(sprintf("Trial %d/%d: score=%.4f (best=%.4f) rounds=%d\n",
                    i, n_iter, score, best_score, nrounds))
      }

    }, error = function(e) {
      if (verbose) {
        cat(sprintf("Trial %d/%d: FAILED - %s\n", i, n_iter, conditionMessage(e)))
      }
      results$score[i] <<- NA
    })
  }

  elapsed <- as.numeric(difftime(Sys.time(), start_time, units = "mins"))

  if (verbose) {
    cat(sprintf("\nTuning complete in %.1f minutes\n", elapsed))
    cat(sprintf("Best score: %.4f at %d rounds\n", best_score, best_nrounds))
    cat("Best parameters:\n")
    for (p_name in names(best_params)) {
      if (is.numeric(best_params[[p_name]])) {
        cat(sprintf("  %s: %.4f\n", p_name, best_params[[p_name]]))
      } else {
        cat(sprintf("  %s: %s\n", p_name, best_params[[p_name]]))
      }
    }
  }

  list(
    best_params = c(fixed_params, best_params),
    best_score = best_score,
    best_nrounds = best_nrounds,
    all_results = results[!is.na(results$score), ],
    tuning_time_mins = elapsed
  )
}
