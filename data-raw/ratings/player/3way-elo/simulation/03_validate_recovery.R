# Validate ELO Recovery ----
#
# Compares the recovered ELO ratings against the true skill values used to
# generate the synthetic data. The key question: Does the ELO system correctly
# identify who the good/bad batters, bowlers, and venues are?
#
# Success criteria:
#   - High correlation between true skills and final ELOs
#   - Correct "direction" of correlation (e.g., good batters have high batter ELO)
#
# If validation fails, this module provides diagnostics to identify the issue.

library(data.table)

# Load configuration
source(file.path(dirname(sys.frame(1)$ofile), "00_simulation_config.R"))

# ============================================================================
# MAIN VALIDATION FUNCTION
# ============================================================================

#' Validate ELO Recovery Against True Skills
#'
#' Joins the true skill values with recovered ELOs and calculates correlations.
#'
#' @param elo_results List from run_elo_on_synthetic().
#' @param players List with batters and bowlers data.tables (true skills).
#' @param venues data.table with venue true skills.
#' @param min_correlation Numeric. Minimum correlation for "success".
#' @param min_venue_correlation Numeric. Minimum correlation for venues.
#'
#' @return List with:
#'   - correlations: Named list of correlation values
#'   - diagnostics: Additional metrics
#'   - success: Logical. Did all correlations meet thresholds?
#'   - batter_results: data.table comparing batter ELOs to true skills
#'   - bowler_results: data.table comparing bowler ELOs to true skills
#'   - venue_results: data.table comparing venue ELOs to true effects
validate_elo_recovery <- function(elo_results,
                                   players,
                                   venues,
                                   min_correlation = SIM_MIN_CORRELATION,
                                   min_venue_correlation = SIM_MIN_VENUE_CORRELATION) {
  final_elos <- elo_results$final_elos

  # --- Batter Validation ---
  batter_results <- data.table::data.table(
    batter_id = players$batters$batter_id,
    true_run_skill = players$batters$true_run_skill,
    true_wicket_rate = players$batters$true_wicket_rate
  )

  # Add final ELOs
  batter_results[, final_run_elo := final_elos$batter_run[batter_id]]
  batter_results[, final_wicket_elo := final_elos$batter_wicket[batter_id]]

  # Calculate correlations
  # Higher true_run_skill should -> higher batter_run_elo (positive correlation)
  cor_batter_run <- cor(batter_results$true_run_skill,
                         batter_results$final_run_elo,
                         use = "complete.obs")

  # Lower true_wicket_rate should -> higher batter_wicket_elo (good survival)
  # So we expect negative correlation with true_wicket_rate
  # Or positive correlation with (1 - true_wicket_rate)
  cor_batter_wicket <- cor(1 - batter_results$true_wicket_rate,
                            batter_results$final_wicket_elo,
                            use = "complete.obs")

  # --- Bowler Validation ---
  bowler_results <- data.table::data.table(
    bowler_id = players$bowlers$bowler_id,
    true_economy = players$bowlers$true_economy,
    true_wicket_rate = players$bowlers$true_wicket_rate
  )

  bowler_results[, final_run_elo := final_elos$bowler_run[bowler_id]]
  bowler_results[, final_wicket_elo := final_elos$bowler_wicket[bowler_id]]

  # Lower true_economy should -> higher bowler_run_elo (gives away fewer runs)
  # So negative correlation with economy, or positive with (1/economy) or (baseline - economy)
  cor_bowler_run <- cor(-bowler_results$true_economy,
                         bowler_results$final_run_elo,
                         use = "complete.obs")

  # Higher true_wicket_rate should -> higher bowler_wicket_elo (takes more wickets)
  cor_bowler_wicket <- cor(bowler_results$true_wicket_rate,
                            bowler_results$final_wicket_elo,
                            use = "complete.obs")

  # --- Venue Validation ---
  venue_results <- data.table::data.table(
    venue = venues$venue,
    true_run_effect = venues$true_run_effect,
    true_wicket_effect = venues$true_wicket_effect
  )

  venue_results[, final_run_elo := final_elos$venue_perm_run[venue]]
  venue_results[, final_wicket_elo := final_elos$venue_perm_wicket[venue]]

  # Higher true_run_effect should -> higher venue_run_elo (high scoring venue)
  # Venue ELO offset from start should correlate with true effect
  cor_venue_run <- cor(venue_results$true_run_effect,
                        venue_results$final_run_elo - THREE_WAY_ELO_START,
                        use = "complete.obs")

  # Higher true_wicket_effect should -> higher venue_wicket_elo
  cor_venue_wicket <- cor(venue_results$true_wicket_effect,
                           venue_results$final_wicket_elo - THREE_WAY_ELO_START,
                           use = "complete.obs")

  # --- Compile Results ---
  correlations <- list(
    batter_run = cor_batter_run,
    batter_wicket = cor_batter_wicket,
    bowler_run = cor_bowler_run,
    bowler_wicket = cor_bowler_wicket,
    venue_run = cor_venue_run,
    venue_wicket = cor_venue_wicket
  )

  # Check success criteria
  player_success <- all(c(
    cor_batter_run >= min_correlation,
    cor_batter_wicket >= min_correlation,
    cor_bowler_run >= min_correlation,
    cor_bowler_wicket >= min_correlation
  ))

  venue_success <- all(c(
    cor_venue_run >= min_venue_correlation,
    cor_venue_wicket >= min_venue_correlation
  ))

  overall_success <- player_success && venue_success

  # Diagnostics
  diagnostics <- list(
    batter_run_elo_range = range(batter_results$final_run_elo, na.rm = TRUE),
    batter_run_elo_mean = mean(batter_results$final_run_elo, na.rm = TRUE),
    bowler_run_elo_range = range(bowler_results$final_run_elo, na.rm = TRUE),
    bowler_run_elo_mean = mean(bowler_results$final_run_elo, na.rm = TRUE),
    venue_run_elo_range = range(venue_results$final_run_elo, na.rm = TRUE),
    venue_run_elo_mean = mean(venue_results$final_run_elo, na.rm = TRUE)
  )

  list(
    correlations = correlations,
    diagnostics = diagnostics,
    success = overall_success,
    player_success = player_success,
    venue_success = venue_success,
    batter_results = batter_results,
    bowler_results = bowler_results,
    venue_results = venue_results,
    thresholds = list(
      min_correlation = min_correlation,
      min_venue_correlation = min_venue_correlation
    )
  )
}


# ============================================================================
# REPORTING FUNCTIONS
# ============================================================================

#' Generate Validation Report
#'
#' Prints a formatted report of validation results.
#'
#' @param validation List from validate_elo_recovery().
generate_validation_report <- function(validation) {
  cat("\n")
  cat("==============================================================\n")
  cat("           3-WAY ELO SIMULATION VALIDATION REPORT            \n")
  cat("==============================================================\n\n")

  # Overall result
  if (validation$success) {
    cat("OVERALL: SUCCESS - ELO system correctly recovers true skills\n")
  } else {
    cat("OVERALL: FAILURE - ELO system did NOT correctly recover true skills\n")
  }
  cat("\n")

  # Correlation table
  cat("--- Correlation Analysis ---\n\n")
  cat(sprintf("%-25s %12s %10s %10s\n",
              "Metric", "Correlation", "Threshold", "Status"))
  cat(strrep("-", 60), "\n")

  thresholds <- validation$thresholds
  correlations <- validation$correlations

  # Player correlations
  for (name in c("batter_run", "batter_wicket", "bowler_run", "bowler_wicket")) {
    cor_val <- correlations[[name]]
    threshold <- thresholds$min_correlation
    status <- if (!is.na(cor_val) && cor_val >= threshold) "PASS" else "FAIL"
    cat(sprintf("%-25s %12.4f %10.2f %10s\n", name, cor_val, threshold, status))
  }

  # Venue correlations
  for (name in c("venue_run", "venue_wicket")) {
    cor_val <- correlations[[name]]
    threshold <- thresholds$min_venue_correlation
    status <- if (!is.na(cor_val) && cor_val >= threshold) "PASS" else "FAIL"
    cat(sprintf("%-25s %12.4f %10.2f %10s\n", name, cor_val, threshold, status))
  }

  cat("\n")

  # Diagnostics
  cat("--- ELO Distribution Diagnostics ---\n\n")
  diag <- validation$diagnostics

  cat(sprintf("Batter Run ELO:  range [%.1f, %.1f], mean %.1f\n",
              diag$batter_run_elo_range[1], diag$batter_run_elo_range[2],
              diag$batter_run_elo_mean))
  cat(sprintf("Bowler Run ELO:  range [%.1f, %.1f], mean %.1f\n",
              diag$bowler_run_elo_range[1], diag$bowler_run_elo_range[2],
              diag$bowler_run_elo_mean))
  cat(sprintf("Venue Run ELO:   range [%.1f, %.1f], mean %.1f\n",
              diag$venue_run_elo_range[1], diag$venue_run_elo_range[2],
              diag$venue_run_elo_mean))

  cat("\n")

  # Interpretation
  cat("--- Interpretation ---\n\n")

  if (validation$success) {
    cat("The ELO system correctly identifies:\n")
    cat("  - Good batters (high strike rate) -> high batter run ELO\n")
    cat("  - Surviving batters (low wicket rate) -> high batter wicket ELO\n")
    cat("  - Economical bowlers (low economy) -> high bowler run ELO\n")
    cat("  - Wicket-taking bowlers (high wicket rate) -> high bowler wicket ELO\n")
    cat("  - High-scoring venues -> high venue run ELO\n")
    cat("  - Bowler-friendly venues -> high venue wicket ELO\n")
  } else {
    cat("DEBUGGING GUIDANCE:\n\n")

    if (correlations$batter_run < thresholds$min_correlation) {
      cat("  ISSUE: batter_run correlation low (%.3f)\n", correlations$batter_run)
      cat("    - Check batter attribution weight (w_batter)\n")
      cat("    - Check K-factor is not too low for batters\n")
      cat("    - Check the ELO update direction (+delta for batter)\n")
    }

    if (correlations$bowler_run < thresholds$min_correlation) {
      cat("  ISSUE: bowler_run correlation low (%.3f)\n", correlations$bowler_run)
      cat("    - Check bowler attribution weight (w_bowler)\n")
      cat("    - Check ELO update is INVERTED for bowler (-delta)\n")
      cat("    - Lower economy should give HIGHER run ELO\n")
    }

    if (correlations$venue_run < thresholds$min_venue_correlation) {
      cat("  ISSUE: venue_run correlation low (%.3f)\n", correlations$venue_run)
      cat("    - Check venue K-factors (might be too low)\n")
      cat("    - Venues need many deliveries to learn effects\n")
      cat("    - Consider increasing venue K or sample size\n")
    }
  }

  cat("\n")
  cat("==============================================================\n")
}


#' Plot Validation Scatter Plots
#'
#' Creates scatter plots of true skills vs recovered ELOs.
#' Requires ggplot2.
#'
#' @param validation List from validate_elo_recovery().
#' @param save_path Character or NULL. If provided, saves plot to file.
#'
#' @return ggplot object (if ggplot2 is available).
plot_validation_results <- function(validation, save_path = NULL) {
  if (!requireNamespace("ggplot2", quietly = TRUE)) {
    cat("ggplot2 not available - skipping plots\n")
    return(invisible(NULL))
  }

  library(ggplot2)

  # Batter plot
  p_batter <- ggplot(validation$batter_results,
                      aes(x = true_run_skill, y = final_run_elo)) +
    geom_point(alpha = 0.7, size = 3) +
    geom_smooth(method = "lm", se = FALSE, color = "red") +
    labs(
      title = sprintf("Batter Run: True Skill vs Recovered ELO (r = %.3f)",
                      validation$correlations$batter_run),
      x = "True Run Skill (runs/ball)",
      y = "Final Batter Run ELO"
    ) +
    theme_minimal()

  # Bowler plot
  p_bowler <- ggplot(validation$bowler_results,
                      aes(x = -true_economy, y = final_run_elo)) +
    geom_point(alpha = 0.7, size = 3, color = "blue") +
    geom_smooth(method = "lm", se = FALSE, color = "red") +
    labs(
      title = sprintf("Bowler Run: -Economy vs Recovered ELO (r = %.3f)",
                      validation$correlations$bowler_run),
      x = "-True Economy (inverted: higher = better)",
      y = "Final Bowler Run ELO"
    ) +
    theme_minimal()

  # Venue plot
  p_venue <- ggplot(validation$venue_results,
                     aes(x = true_run_effect, y = final_run_elo - THREE_WAY_ELO_START)) +
    geom_point(alpha = 0.7, size = 4, color = "darkgreen") +
    geom_smooth(method = "lm", se = FALSE, color = "red") +
    labs(
      title = sprintf("Venue Run: True Effect vs ELO Offset (r = %.3f)",
                      validation$correlations$venue_run),
      x = "True Run Effect",
      y = "Venue Run ELO Offset from Start"
    ) +
    theme_minimal()

  if (requireNamespace("patchwork", quietly = TRUE)) {
    library(patchwork)
    combined <- p_batter / p_bowler / p_venue
    print(combined)
    if (!is.null(save_path)) {
      ggsave(save_path, combined, width = 10, height = 12)
    }
    return(invisible(combined))
  } else {
    print(p_batter)
    print(p_bowler)
    print(p_venue)
    return(invisible(list(batter = p_batter, bowler = p_bowler, venue = p_venue)))
  }
}


# ============================================================================
# DETAILED DIAGNOSTICS
# ============================================================================

#' Analyze Entity-Level Performance
#'
#' Shows which entities were correctly/incorrectly ranked.
#'
#' @param validation List from validate_elo_recovery().
#' @param top_n Integer. How many top/bottom entities to show.
analyze_entity_rankings <- function(validation, top_n = 5) {
  cat("\n=== Entity Ranking Analysis ===\n\n")

  # Batter analysis
  batter_ranked <- validation$batter_results[order(-true_run_skill)]
  batter_ranked[, true_rank := .I]
  batter_ranked <- batter_ranked[order(-final_run_elo)]
  batter_ranked[, elo_rank := .I]
  batter_ranked[, rank_diff := abs(true_rank - elo_rank)]

  cat("Top 5 TRUE best batters (by skill) vs their ELO rank:\n")
  print(head(batter_ranked[order(true_rank)][, .(batter_id, true_run_skill, final_run_elo,
                                                   true_rank, elo_rank, rank_diff)], top_n))

  cat("\nTop 5 ELO batters vs their true rank:\n")
  print(head(batter_ranked[order(elo_rank)][, .(batter_id, true_run_skill, final_run_elo,
                                                  true_rank, elo_rank, rank_diff)], top_n))

  # Bowler analysis
  bowler_ranked <- validation$bowler_results[order(true_economy)]  # Lower is better
  bowler_ranked[, true_rank := .I]
  bowler_ranked <- bowler_ranked[order(-final_run_elo)]  # Higher ELO is better
  bowler_ranked[, elo_rank := .I]
  bowler_ranked[, rank_diff := abs(true_rank - elo_rank)]

  cat("\nTop 5 TRUE best bowlers (by economy) vs their ELO rank:\n")
  print(head(bowler_ranked[order(true_rank)][, .(bowler_id, true_economy, final_run_elo,
                                                   true_rank, elo_rank, rank_diff)], top_n))

  invisible(list(batter_ranked = batter_ranked, bowler_ranked = bowler_ranked))
}


#' Check for Common Issues
#'
#' Diagnoses potential problems in the ELO recovery.
#'
#' @param validation List from validate_elo_recovery().
#' @param elo_results List from run_elo_on_synthetic().
diagnose_issues <- function(validation, elo_results) {
  cat("\n=== Issue Diagnosis ===\n\n")

  params <- elo_results$params
  issues_found <- FALSE

  # Check 1: ELO range too narrow
  diag <- validation$diagnostics
  batter_range <- diff(diag$batter_run_elo_range)
  if (batter_range < 50) {
    cat("ISSUE: Batter ELO range too narrow (%.1f points)\n", batter_range)
    cat("  -> K-factors may be too low, or not enough deliveries\n")
    issues_found <- TRUE
  }

  # Check 2: Mean drift
  if (abs(diag$batter_run_elo_mean - params$elo_start) > 100) {
    cat("ISSUE: Batter ELO mean drifted significantly (%.1f from start)\n",
        diag$batter_run_elo_mean - params$elo_start)
    cat("  -> May need normalization or check update formulas\n")
    issues_found <- TRUE
  }

  # Check 3: Inverted correlations (sign error)
  cors <- validation$correlations
  if (cors$batter_run < -0.3) {
    cat("ISSUE: Batter run correlation is NEGATIVE (%.3f)\n", cors$batter_run)
    cat("  -> Check if ELO update direction is correct (should be +delta for batter)\n")
    issues_found <- TRUE
  }
  if (cors$bowler_run < -0.3) {
    cat("ISSUE: Bowler run correlation is NEGATIVE (%.3f)\n", cors$bowler_run)
    cat("  -> Check if bowler update is inverted correctly (-delta)\n")
    issues_found <- TRUE
  }

  # Check 4: Very low venue correlation
  if (cors$venue_run < 0.1 && !is.na(cors$venue_run)) {
    cat("ISSUE: Venue run correlation is very low (%.3f)\n", cors$venue_run)
    cat("  -> Venues may need more data or higher K-factors\n")
    cat("  -> Venue effects are small - may need more deliveries per venue\n")
    issues_found <- TRUE
  }

  if (!issues_found) {
    cat("No major issues detected.\n")
  }
}

NULL
