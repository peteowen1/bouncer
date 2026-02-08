# 3-Way Glicko Rating System
#
# A Glicko-style rating system for cricket that extends the 3-way ELO approach
# by adding explicit uncertainty tracking via Rating Deviation (RD).
#
# Key differences from standard ELO:
#   - ELO: Implicit uncertainty via K-factor decay with experience
#   - Glicko: Explicit uncertainty (RD) that increases with inactivity
#            and decreases with each observation
#
# Mathematical foundation:
#   - Uses exponential link function: λ = base * 10^(θ/400)
#   - Poisson variance: variance ∝ mean * φ (overdispersion parameter)
#   - g-function weights updates by opponent uncertainty
#   - RD derived from inverse Fisher Information
#
# Three entities participate:
#   - Batter: Higher rating = scores more runs
#   - Bowler: Higher rating = concedes fewer runs
#   - Venue: Higher rating = more runs scored (batting-friendly)

# ============================================================================
# CORE GLICKO FUNCTIONS
# ============================================================================

#' Calculate Glicko g-function
#'
#' The g-function downweights rating updates when the opponent has high
#' uncertainty (high RD). This prevents large swings based on unreliable
#' information.
#'
#' Formula: g(RD) = 1 / sqrt(1 + 3 * Q² * RD² / π²)
#'
#' Properties:
#'   - g(0) = 1 (perfectly known opponent = full weight)
#'   - g(350) ≈ 0.85 (standard starting RD)
#'   - g(500) ≈ 0.75 (uncertain opponent = reduced weight)
#'
#' @param rd Numeric. Rating Deviation of the opponent(s).
#' @param q Numeric. Glicko constant, default GLICKO_Q (ln(10)/400).
#'
#' @return Numeric. g-function value between 0 and 1.
#' @keywords internal
glicko_g <- function(rd, q = GLICKO_Q) {
  1 / sqrt(1 + 3 * q^2 * rd^2 / pi^2)
}


#' Calculate Composite RD
#'
#' Combines multiple Rating Deviations into a single composite uncertainty.
#' Used when facing multiple entities (e.g., bowler + venue).
#'
#' Formula: RD_composite = sqrt(RD_1² + RD_2² + ...)
#'
#' This assumes independence of uncertainties, which is reasonable for
#' separate entities (bowler skill vs venue characteristics).
#'
#' @param rds Numeric vector. Rating Deviations to combine.
#'
#' @return Numeric. Composite RD.
#' @keywords internal
glicko_composite_rd <- function(rds) {
  sqrt(sum(rds^2, na.rm = TRUE))
}


#' Calculate Expected Runs (Exponential Link)
#'
#' Uses exponential link function to convert combined rating (θ) to expected
#' runs per ball. This is more appropriate than linear for count data.
#'
#' Formula: λ = base_runs * 10^(θ / 400)
#'
#' Where θ = (R_batter + R_venue) - R_bowler - neutral_anchor
#'
#' The neutral_anchor ensures θ = 0 when all ratings are at 1500.
#'
#' @param theta Numeric. Combined rating difference.
#' @param base_runs Numeric. Format-specific base runs per ball.
#'
#' @return Numeric. Expected runs (λ parameter for Poisson).
#' @keywords internal
glicko_expected_runs <- function(theta, base_runs) {

  lambda <- base_runs * 10^(theta / 400)
  # Bound to reasonable range (Poisson mean can't be negative)
  pmax(0.01, pmin(6, lambda))
}


#' Calculate d² (Inverse Fisher Information for Poisson)
#'
#' In Glicko, d² represents the information gained from an observation.
#' For Poisson outcomes, this depends on the expected value (λ) and
#' the overdispersion parameter (φ).
#'
#' Formula: d² = 1 / (Q² * g(RD_opp)² * λ * φ)
#'
#' Higher d² means less information gained (and smaller RD reduction).
#' This happens when:
#'   - Opponent uncertainty is high (g is low)
#'   - Expected runs are low (less variance to learn from)
#'   - Overdispersion is low (less signal in variance)
#'
#' @param rd_opp Numeric. Opponent's (or composite) RD.
#' @param lambda Numeric. Expected runs (Poisson mean).
#' @param phi Numeric. Overdispersion parameter (φ > 1 for cricket).
#' @param q Numeric. Glicko constant, default GLICKO_Q.
#'
#' @return Numeric. d² value.
#' @keywords internal
glicko_d_squared <- function(rd_opp, lambda, phi, q = GLICKO_Q) {
  g_val <- glicko_g(rd_opp, q)
  # Prevent division by zero
  lambda <- pmax(lambda, 0.01)
  phi <- pmax(phi, 0.1)
  1 / (q^2 * g_val^2 * lambda * phi)
}


#' Update Rating Deviation After Observation
#'
#' After each delivery, RD decreases (we're more confident about the rating).
#' The amount of decrease depends on d² (information gained).
#'
#' Formula: new_RD = sqrt(1 / (1/old_RD² + 1/d²))
#'
#' This is the standard Glicko RD update, equivalent to Bayesian precision
#' update where precision = 1/variance.
#'
#' @param old_rd Numeric. Current Rating Deviation.
#' @param d_sq Numeric. d² from the observation.
#' @param rd_min Numeric. Minimum RD floor.
#' @param rd_max Numeric. Maximum RD cap.
#'
#' @return Numeric. Updated RD.
#' @keywords internal
glicko_update_rd <- function(old_rd, d_sq,
                              rd_min = GLICKO_RD_MIN,
                              rd_max = GLICKO_RD_MAX) {
  # RD always decreases after observation (more confidence)
  new_variance <- 1 / (1 / old_rd^2 + 1 / d_sq)
  new_rd <- sqrt(new_variance)
  # Apply bounds
  pmax(rd_min, pmin(rd_max, new_rd))
}


#' Update Glicko Rating After Observation
#'
#' Updates the rating based on the prediction error, weighted by:
#'   - Current uncertainty (RD): Higher RD = larger updates
#'   - Opponent uncertainty: Higher opponent RD = smaller updates (g-function)
#'   - Information gained: d² from Poisson variance
#'
#' Formula: update = (Q / (1/old_RD² + 1/d²)) * g(RD_opp) * error
#'          new_R = old_R + update
#'
#' @param old_r Numeric. Current rating.
#' @param old_rd Numeric. Current Rating Deviation.
#' @param error Numeric. Prediction error (actual - expected).
#' @param rd_opp Numeric. Opponent's (or composite) RD.
#' @param d_sq Numeric. d² from the observation.
#' @param q Numeric. Glicko constant, default GLICKO_Q.
#'
#' @return Numeric. Updated rating.
#' @keywords internal
glicko_update_rating <- function(old_r, old_rd, error, rd_opp, d_sq,
                                  q = GLICKO_Q) {
  g_val <- glicko_g(rd_opp, q)
  # Weight by both current uncertainty and information gained
  update_factor <- q / (1 / old_rd^2 + 1 / d_sq)
  old_r + update_factor * g_val * error
}


#' Decay RD During Inactivity
#'
#' When a player hasn't played, uncertainty increases (we're less sure
#' their rating still reflects their current ability).
#'
#' Formula: new_RD = sqrt(old_RD² + decay_rate² * days_inactive)
#'
#' @param rd Numeric. Current Rating Deviation.
#' @param days_inactive Integer. Days since last match.
#' @param decay_per_day Numeric. RD growth rate per day.
#' @param rd_max Numeric. Maximum RD cap.
#'
#' @return Numeric. Decayed (increased) RD.
#' @keywords internal
glicko_decay_rd <- function(rd, days_inactive,
                             decay_per_day = GLICKO_RD_DECAY_PER_DAY,
                             rd_max = GLICKO_RD_MAX) {
  if (days_inactive <= 0) return(rd)
  new_rd <- sqrt(rd^2 + decay_per_day^2 * days_inactive)
  pmin(new_rd, rd_max)
}


# ============================================================================
# 3-WAY COMBINED RATING CALCULATION
# ============================================================================

#' Calculate 3-Way Combined Rating (θ)
#'
#' Combines batter, bowler, and venue ratings into a single value
#' that determines expected runs.
#'
#' Formula: θ = w_bat * (R_bat - 1500) + w_venue * (R_venue - 1500)
#'              - w_bowl * (R_bowl - 1500)
#'
#' Batter and venue positively contribute (higher = more runs).
#' Bowler negatively contributes (higher bowler rating = fewer runs).
#'
#' @param batter_r Numeric. Batter's rating.
#' @param bowler_r Numeric. Bowler's rating.
#' @param venue_r Numeric. Venue's rating.
#' @param w_batter Numeric. Batter weight, default GLICKO_W_BATTER.
#' @param w_bowler Numeric. Bowler weight, default GLICKO_W_BOWLER.
#' @param w_venue Numeric. Venue weight, default GLICKO_W_VENUE.
#' @param neutral Numeric. Neutral rating anchor, default 1500.
#'
#' @return Numeric. Combined rating θ.
#' @keywords internal
calculate_3way_glicko_theta <- function(batter_r, bowler_r, venue_r,
                                         w_batter = GLICKO_W_BATTER,
                                         w_bowler = GLICKO_W_BOWLER,
                                         w_venue = GLICKO_W_VENUE,
                                         neutral = 1500) {
  # Batter and venue contribute positively (higher = more runs)
  # Bowler contributes negatively (higher bowler rating = fewer runs)
  w_batter * (batter_r - neutral) +
    w_venue * (venue_r - neutral) -
    w_bowler * (bowler_r - neutral)
}


# ============================================================================
# FULL 3-WAY UPDATE FUNCTION
# ============================================================================

#' Update 3-Way Glicko Ratings for a Single Delivery
#'
#' Performs a complete Glicko update for batter, bowler, and venue
#' based on a single delivery outcome.
#'
#' Steps:
#'   1. Calculate composite RD for opponent perspective
#'   2. Calculate expected runs from combined rating
#'   3. Compute prediction error
#'   4. Calculate d² (information gained)
#'   5. Update RDs (always decrease with observation)
#'   6. Update ratings (proportional to error and uncertainty)
#'
#' @param actual_runs Integer. Actual runs scored on delivery.
#' @param batter_r Numeric. Batter's current rating.
#' @param batter_rd Numeric. Batter's current RD.
#' @param bowler_r Numeric. Bowler's current rating.
#' @param bowler_rd Numeric. Bowler's current RD.
#' @param venue_r Numeric. Venue's current rating.
#' @param venue_rd Numeric. Venue's current RD.
#' @param base_runs Numeric. Format-specific base runs per ball.
#' @param phi Numeric. Overdispersion parameter for Poisson.
#' @param w_batter Numeric. Batter weight.
#' @param w_bowler Numeric. Bowler weight.
#' @param w_venue Numeric. Venue weight.
#'
#' @return List with updated ratings and RDs for all three entities.
#' @keywords internal
update_3way_glicko <- function(actual_runs,
                                batter_r, batter_rd,
                                bowler_r, bowler_rd,
                                venue_r, venue_rd,
                                base_runs,
                                phi,
                                w_batter = GLICKO_W_BATTER,
                                w_bowler = GLICKO_W_BOWLER,
                                w_venue = GLICKO_W_VENUE) {

  # Step 1: Calculate combined rating (θ)
  theta <- calculate_3way_glicko_theta(batter_r, bowler_r, venue_r,
                                        w_batter, w_bowler, w_venue)

  # Step 2: Calculate expected runs using exponential link
  lambda <- glicko_expected_runs(theta, base_runs)

  # Step 3: Calculate prediction error
  error <- actual_runs - lambda

  # Step 4: Calculate composite RDs for each entity's perspective
  # Batter sees: bowler + venue uncertainty
  rd_opp_batter <- glicko_composite_rd(c(bowler_rd, venue_rd))
  # Bowler sees: batter + venue uncertainty
  rd_opp_bowler <- glicko_composite_rd(c(batter_rd, venue_rd))
  # Venue sees: batter + bowler uncertainty
  rd_opp_venue <- glicko_composite_rd(c(batter_rd, bowler_rd))

  # Step 5: Calculate d² for each entity
  d_sq_batter <- glicko_d_squared(rd_opp_batter, lambda, phi)
  d_sq_bowler <- glicko_d_squared(rd_opp_bowler, lambda, phi)
  d_sq_venue <- glicko_d_squared(rd_opp_venue, lambda, phi)

  # Step 6: Update RDs (always decrease after observation)
  new_batter_rd <- glicko_update_rd(batter_rd, d_sq_batter)
  new_bowler_rd <- glicko_update_rd(bowler_rd, d_sq_bowler)
  new_venue_rd <- glicko_update_rd(venue_rd, d_sq_venue,
                                    rd_min = GLICKO_VENUE_RD_MIN,
                                    rd_max = GLICKO_VENUE_RD_MAX)

  # Step 7: Update ratings
  # Batter: positive error = scored well = rating increases
  new_batter_r <- glicko_update_rating(batter_r, batter_rd, error,
                                        rd_opp_batter, d_sq_batter)

  # Bowler: positive error = conceded too many = rating decreases
  new_bowler_r <- glicko_update_rating(bowler_r, bowler_rd, -error,
                                        rd_opp_bowler, d_sq_bowler)

  # Venue: positive error = high-scoring venue = rating increases
  new_venue_r <- glicko_update_rating(venue_r, venue_rd, error,
                                       rd_opp_venue, d_sq_venue)

  list(
    # Updated ratings
    batter_r = new_batter_r,
    bowler_r = new_bowler_r,
    venue_r = new_venue_r,
    # Updated RDs
    batter_rd = new_batter_rd,
    bowler_rd = new_bowler_rd,
    venue_rd = new_venue_rd,
    # Diagnostics
    expected_runs = lambda,
    theta = theta,
    error = error
  )
}


# ============================================================================
# FORMAT-SPECIFIC PARAMETER FUNCTIONS
# ============================================================================

#' Get Glicko Parameters for Format
#'
#' Returns format-specific Glicko parameters.
#'
#' @param format Character. Match format: "t20", "odi", or "test".
#'
#' @return List with base_runs, phi, and other format parameters.
#' @keywords internal
get_glicko_params <- function(format = "t20") {
  format <- tolower(format)

  switch(format,
    "t20" = list(
      base_runs = GLICKO_BASE_RUNS_T20,
      phi = GLICKO_PHI_T20,
      rd_start = GLICKO_RD_START,
      rd_min = GLICKO_RD_MIN,
      rd_max = GLICKO_RD_MAX,
      rd_decay = GLICKO_RD_DECAY_PER_DAY,
      venue_rd_start = GLICKO_VENUE_RD_START,
      venue_rd_min = GLICKO_VENUE_RD_MIN,
      venue_rd_max = GLICKO_VENUE_RD_MAX,
      venue_rd_decay = GLICKO_VENUE_RD_DECAY_PER_DAY
    ),
    "odi" = list(
      base_runs = GLICKO_BASE_RUNS_ODI,
      phi = GLICKO_PHI_ODI,
      rd_start = GLICKO_RD_START,
      rd_min = GLICKO_RD_MIN,
      rd_max = GLICKO_RD_MAX,
      rd_decay = GLICKO_RD_DECAY_PER_DAY,
      venue_rd_start = GLICKO_VENUE_RD_START,
      venue_rd_min = GLICKO_VENUE_RD_MIN,
      venue_rd_max = GLICKO_VENUE_RD_MAX,
      venue_rd_decay = GLICKO_VENUE_RD_DECAY_PER_DAY
    ),
    "test" = list(
      base_runs = GLICKO_BASE_RUNS_TEST,
      phi = GLICKO_PHI_TEST,
      rd_start = GLICKO_RD_START,
      rd_min = GLICKO_RD_MIN,
      rd_max = GLICKO_RD_MAX,
      rd_decay = GLICKO_RD_DECAY_PER_DAY,
      venue_rd_start = GLICKO_VENUE_RD_START,
      venue_rd_min = GLICKO_VENUE_RD_MIN,
      venue_rd_max = GLICKO_VENUE_RD_MAX,
      venue_rd_decay = GLICKO_VENUE_RD_DECAY_PER_DAY
    ),
    # Default to T20
    list(
      base_runs = GLICKO_BASE_RUNS_T20,
      phi = GLICKO_PHI_T20,
      rd_start = GLICKO_RD_START,
      rd_min = GLICKO_RD_MIN,
      rd_max = GLICKO_RD_MAX,
      rd_decay = GLICKO_RD_DECAY_PER_DAY,
      venue_rd_start = GLICKO_VENUE_RD_START,
      venue_rd_min = GLICKO_VENUE_RD_MIN,
      venue_rd_max = GLICKO_VENUE_RD_MAX,
      venue_rd_decay = GLICKO_VENUE_RD_DECAY_PER_DAY
    )
  )
}


# ============================================================================
# POISSON LOSS FUNCTION
# ============================================================================

#' Calculate Poisson Loss
#'
#' The standard loss function for Poisson regression.
#' Lower is better.
#'
#' Formula: mean(λ - y * log(λ))
#'
#' This is the negative log-likelihood of Poisson, ignoring constants.
#'
#' @param actual Numeric vector. Actual run outcomes.
#' @param expected Numeric vector. Expected runs (Poisson λ).
#'
#' @return Numeric. Mean Poisson loss.
#' @keywords internal
poisson_loss <- function(actual, expected) {
  expected <- pmax(expected, 1e-6)  # Prevent log(0)
  mean(expected - actual * log(expected))
}


#' Calculate RMSE
#'
#' Root Mean Squared Error for comparison with other metrics.
#'
#' @param actual Numeric vector. Actual outcomes.
#' @param expected Numeric vector. Expected values.
#'
#' @return Numeric. RMSE.
#' @keywords internal
calculate_rmse <- function(actual, expected) {
  sqrt(mean((actual - expected)^2))
}


#' Calculate MAE
#'
#' Mean Absolute Error for comparison with other metrics.
#'
#' @param actual Numeric vector. Actual outcomes.
#' @param expected Numeric vector. Expected values.
#'
#' @return Numeric. MAE.
#' @keywords internal
calculate_mae <- function(actual, expected) {
  mean(abs(actual - expected))
}


# ============================================================================
# EFFECTIVE ELO COMPARISON
# ============================================================================

#' Convert Glicko RD to Effective K-factor
#'
#' For comparison with standard ELO, converts the Glicko RD to an
#' equivalent K-factor.
#'
#' The Glicko update effective K is proportional to 1/RD².
#' A player with RD=350 has roughly the same update magnitude as
#' K ≈ 0.002 (very small), while RD=100 corresponds to K ≈ 0.025.
#'
#' This is a rough approximation for intuition only.
#'
#' @param rd Numeric. Current Rating Deviation.
#' @param q Numeric. Glicko constant.
#'
#' @return Numeric. Approximate effective K-factor.
#' @keywords internal
glicko_rd_to_effective_k <- function(rd, q = GLICKO_Q) {
  # Based on Glicko formula: update_factor = Q / (1/RD² + 1/d²)

# When d² >> RD², update_factor ≈ Q * RD²
  # This gives a sense of the "learning rate"
  q * rd^2 / 1000  # Scale for intuition
}


NULL
