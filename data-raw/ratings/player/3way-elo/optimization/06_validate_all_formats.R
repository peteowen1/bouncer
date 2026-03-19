# Validate 3-Way ELO Across All Format-Gender Combinations ----
#
# Runs 05_validate_3way_elo.R for every format-gender pair and
# prints a consolidated comparison table.
#
# Usage:
#   source("data-raw/ratings/player/3way-elo/optimization/06_validate_all_formats.R")

library(DBI)
library(data.table)
devtools::load_all()

FORMATS <- c("t20", "odi", "test")
GENDERS <- c("male", "female")

cat("\n")
cli::cli_h1("3-Way ELO Cross-Format Validation")
cat("\n")

validation_dir <- file.path(
  dirname(rstudioapi::getSourceEditorContext()$path %||% ""),
  "."
)
# Fallback if not in RStudio
if (!file.exists(file.path(validation_dir, "05_validate_3way_elo.R"))) {
  validation_dir <- file.path(getwd(), "data-raw/ratings/player/3way-elo/optimization")
}

all_summaries <- list()

for (fmt in FORMATS) {
  for (gender in GENDERS) {
    combo <- paste(gender, fmt)
    cli::cli_rule("{toupper(combo)}")

    # Check if 3-way ELO table exists for this format
    conn <- get_db_connection(read_only = TRUE)
    elo_table <- paste0(fmt, "_3way_elo")
    has_table <- table_exists(conn, elo_table)
    DBI::dbDisconnect(conn, shutdown = TRUE)

    if (!has_table) {
      cli::cli_alert_warning("No {elo_table} table found, skipping")
      next
    }

    # Set variables and source the validation script
    FORMAT <- fmt
    GENDER <- gender
    VALIDATION_EVENTS <- NULL  # All events
    VALIDATION_YEARS <- c(2023, 2024, 2025)
    USE_BLENDING <- TRUE

    tryCatch({
      source(file.path(validation_dir, "05_validate_3way_elo.R"), local = TRUE)

      all_summaries[[combo]] <- data.frame(
        format = fmt,
        gender = gender,
        poisson_loss = poisson_loss,
        poisson_improvement = poisson_improvement,
        brier_score = brier_score,
        brier_improvement = brier_improvement,
        log_loss = log_loss,
        log_loss_improvement = log_loss_improvement,
        n_deliveries = n_deliveries,
        n_matches = n_matches,
        stringsAsFactors = FALSE
      )
    }, error = function(e) {
      cli::cli_alert_danger("{combo}: {conditionMessage(e)}")
    })
  }
}

# Print consolidated table
if (length(all_summaries) > 0) {
  cat("\n")
  cli::cli_h1("Consolidated Results")

  summary_dt <- data.table::rbindlist(all_summaries)

  cat("\n--- Improvement Over Baseline (%) ---\n")
  print(summary_dt[, .(
    format, gender,
    poisson = sprintf("%+.2f%%", poisson_improvement),
    brier = sprintf("%+.2f%%", brier_improvement),
    logloss = sprintf("%+.2f%%", log_loss_improvement),
    n_deliveries = format(n_deliveries, big.mark = ","),
    n_matches
  )])

  cat("\n")
  cli::cli_alert_info("Positive values = model beats baseline")
  cli::cli_alert_info("Negative values = model WORSE than baseline (investigate!)")
}
