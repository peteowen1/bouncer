# Additive Skill Index System
#
# A unified skill system where Batter, Bowler, and Venue all have directly
# interpretable skill indices based on deviation from expected outcomes.
#
# Two dimensions:
#   - Run Skill: Deviation from expected runs per ball (e.g., +0.30 runs/ball)
#   - Wicket Skill: Deviation from expected wicket probability (e.g., +2%)
#
# Key advantages over ELO:
#   - Directly interpretable (no conversion factor)
#   - Start at 0 (neutral) instead of arbitrary 1400
#   - Continuous decay toward 0 (Bayesian prior)
#   - Simpler formula: expected = baseline + weighted skill sum
#
# Key features:
#   - Dual venue component (permanent + session)
#   - Experience-based alpha decay (like current K-factor system)
#   - Continuous decay toward neutral every delivery
#   - Soft bounds to prevent extreme values

# ============================================================================
# EXPECTED VALUE CALCULATIONS
# ============================================================================

#' Calculate Combined Skill Effect (Run)
#'
#' Computes the weighted combined skill effect for batter, bowler, and venue.
#' This is the adjustment to baseline expected runs.
#'
#' Formula:
#'   combined_effect = w_batter * batter_skill +
#'                     w_bowler * (-bowler_skill) +  # Inverted: good bowler reduces runs
#'                     w_venue_perm * venue_perm_skill +
#'                     w_venue_session * venue_session_skill
#'
#' @param batter_run_skill Numeric. Batter's run skill index.
#' @param bowler_run_skill Numeric. Bowler's run skill index.
#' @param venue_perm_run_skill Numeric. Venue's permanent run skill index.
#' @param venue_session_run_skill Numeric. Venue's session run skill index.
#' @param format Character. Match format.
#' @param gender Character. Gender.
#'
#' @return Numeric. Combined weighted skill effect on runs.
#' @keywords internal
calculate_combined_skill_effect <- function(batter_run_skill,
                                             bowler_run_skill,
                                             venue_perm_run_skill,
                                             venue_session_run_skill,
                                             format = "t20",
                                             gender = "male") {
  weights <- get_skill_weights(format, gender, "run")

  # Batter: positive skill = more runs (positive contribution)
  # Bowler: positive skill = RESTRICTS runs (inverted - subtract)
  # Venue: positive skill = more runs (high-scoring venue)
  weights$w_batter * batter_run_skill -
    weights$w_bowler * bowler_run_skill +
    weights$w_venue_perm * venue_perm_run_skill +
    weights$w_venue_session * venue_session_run_skill
}


#' Calculate Expected Runs (Skill Index)
#'
#' Uses agnostic model as baseline with skill-based adjustments from
#' batter, bowler, and venue skill indices.
#'
#' Formula:
#'   expected_runs = baseline + combined_skill_effect
#'
#' @param agnostic_runs Numeric. Base expected runs from agnostic model.
#' @param batter_run_skill Numeric. Batter's run skill index.
#' @param bowler_run_skill Numeric. Bowler's run skill index.
#' @param venue_perm_run_skill Numeric. Venue's permanent run skill index.
#' @param venue_session_run_skill Numeric. Venue's session run skill index.
#' @param format Character. Match format: "t20", "odi", or "test".
#' @param gender Character. Gender: "male" or "female". Default "male".
#'
#' @return Numeric. Adjusted expected runs.
#'
#' @examples
#' calculate_expected_runs_skill(1.1, 0.2, -0.1, 0.05, 0.0)
#'
#' @export
calculate_expected_runs_skill <- function(agnostic_runs,
                                           batter_run_skill,
                                           bowler_run_skill,
                                           venue_perm_run_skill,
                                           venue_session_run_skill,
                                           format = "t20",
                                           gender = "male") {
  format <- tolower(format)

  # Calculate combined skill effect
  skill_effect <- calculate_combined_skill_effect(
    batter_run_skill, bowler_run_skill,
    venue_perm_run_skill, venue_session_run_skill,
    format, gender
  )

  # Simple additive formula: baseline + skill effect
  expected_runs <- agnostic_runs + skill_effect

  # Bound to reasonable range (0 to 6 runs per ball)
  pmax(0, pmin(6, expected_runs))
}


#' Calculate Expected Wicket Probability (Skill Index)
#'
#' Uses agnostic model as baseline with skill-based adjustments.
#' Skills are added DIRECTLY to probability (no weighting in prediction).
#' Weights are only used for distributing updates, not for prediction.
#'
#' Formula:
#'   expected_wicket = clamp(baseline + batter_wicket_skill + bowler_wicket_skill +
#'                           venue_perm_wicket_skill + venue_session_wicket_skill,
#'                           0.001, 0.50)
#'
#' NOTE: Wicket skills use OPPOSITE sign convention from run skills.
#' Run skills:    positive = good for entity (batter scores more / bowler restricts more)
#' Wicket skills: positive = higher wicket probability for BOTH (bad for batter, good for bowler)
#' This is intentional - each skill represents DIRECT contribution to wicket probability.
#'
#' Batter wicket skill is positive if they get out MORE often (bad for batter).
#' Bowler wicket skill is positive if they take MORE wickets (good for bowler).
#'
#' @param agnostic_wicket Numeric. Base wicket probability from agnostic model.
#' @param batter_wicket_skill Numeric. Batter's wicket skill index (positive = gets out more).
#' @param bowler_wicket_skill Numeric. Bowler's wicket skill index (positive = takes more wickets).
#' @param venue_perm_wicket_skill Numeric. Venue permanent wicket skill index.
#' @param venue_session_wicket_skill Numeric. Venue session wicket skill index.
#' @param format Character. Match format (unused, kept for API consistency).
#' @param gender Character. Gender (unused, kept for API consistency).
#'
#' @return Numeric. Adjusted expected wicket probability.
#'
#' @examples
#' calculate_expected_wicket_skill(0.054, 0.01, 0.02, 0.005, 0.0)
#'
#' @export
calculate_expected_wicket_skill <- function(agnostic_wicket,
                                             batter_wicket_skill,
                                             bowler_wicket_skill,
                                             venue_perm_wicket_skill,
                                             venue_session_wicket_skill,
                                             format = "t20",
                                             gender = "male") {
  # Direct additive skill effect - NO weighting in prediction!
  # Weights are for UPDATE distribution only, not for prediction.
  # This ensures skill values are directly interpretable as probability changes.
  #
  # Each skill represents the entity's DIRECT contribution:
  # - Batter: positive = gets out more (adds to wicket prob)
  # - Bowler: positive = takes more wickets (adds to wicket prob)
  # - Venue: positive = more wickets fall here
  skill_effect <- batter_wicket_skill +
    bowler_wicket_skill +
    venue_perm_wicket_skill +
    venue_session_wicket_skill

  # Additive adjustment to baseline probability
  expected_wicket <- agnostic_wicket + skill_effect

  # Bound to valid probability range
  pmax(0.001, pmin(0.50, expected_wicket))
}


# ============================================================================
# LEARNING RATE (ALPHA) CALCULATIONS
# ============================================================================

#' Get Skill Alpha (Learning Rate)
#'
#' Calculates the learning rate for skill updates based on experience.
#' New players learn faster, experienced players have more stable ratings.
#'
#' Formula:
#'   alpha = alpha_min + (alpha_max - alpha_min) * exp(-deliveries / halflife)
#'
#' Examples (Men's T20 Run):
#'   0 balls:   alpha = 0.05 (max, fast learning)
#'   300 balls: alpha ~ 0.03 (halfway to min)
#'   1000 balls: alpha ~ 0.01 (near min, stable)
#'
#' @param deliveries Integer. Number of deliveries faced/bowled.
#' @param format Character. Match format: "t20", "odi", or "test".
#' @param skill_type Character. "run" or "wicket".
#' @param gender Character. Gender: "male" or "female". Default "male".
#'
#' @return Numeric. The learning rate (alpha) for this player.
#'
#' @examples
#' get_skill_alpha(0, "t20")     # New player: max alpha
#' get_skill_alpha(300, "t20")   # Experienced: mid alpha
#' get_skill_alpha(1000, "t20")  # Veteran: near min alpha
#'
#' @export
get_skill_alpha <- function(deliveries,
                             format = "t20",
                             skill_type = "run",
                             gender = "male") {
  params <- get_skill_alpha_params(format, gender, skill_type)

  params$alpha_min + (params$alpha_max - params$alpha_min) *
    exp(-deliveries / params$halflife)
}


#' Get Venue Skill Alpha (Permanent)
#'
#' Calculates the learning rate for permanent venue skill.
#' Venues learn slowly as their characteristics are relatively stable.
#'
#' @param venue_total_balls Integer. Total balls at this venue.
#' @param format Character. Match format.
#'
#' @return Numeric. The alpha for permanent venue skill.
#' @keywords internal
get_venue_perm_skill_alpha <- function(venue_total_balls, format = "t20") {
  format <- tolower(format)

  base_alpha <- switch(format,
    "t20" = SKILL_VENUE_ALPHA_PERM_T20,
    "odi" = SKILL_VENUE_ALPHA_PERM_ODI,
    "test" = SKILL_VENUE_ALPHA_PERM_TEST,
    SKILL_VENUE_ALPHA_PERM_T20
  )

  # Alpha decays slightly with more balls (venue becomes more established)
  # But doesn't decay below 20% of base alpha
  max(base_alpha * 0.2, base_alpha * exp(-venue_total_balls / 10000))
}


#' Get Venue Session Skill Alpha
#'
#' Calculates the learning rate for session venue skill.
#' Session skill learns fast within a match to capture current conditions.
#'
#' @param balls_in_match Integer. Balls bowled in current match.
#'
#' @return Numeric. The alpha for session venue skill.
#' @keywords internal
get_venue_session_skill_alpha <- function(balls_in_match) {
  SKILL_VENUE_ALPHA_SESSION_MIN +
    (SKILL_VENUE_ALPHA_SESSION_MAX - SKILL_VENUE_ALPHA_SESSION_MIN) *
    exp(-balls_in_match / SKILL_VENUE_ALPHA_SESSION_HALFLIFE)
}


# ============================================================================
# SKILL UPDATE FUNCTIONS
# ============================================================================

#' Apply Continuous Decay to Skill
#'
#' Applies a small decay toward 0 (neutral) on every delivery.
#' This provides a Bayesian prior that pulls skills toward the mean.
#'
#' Formula:
#'   decayed_skill = skill * (1 - decay_rate)
#'
#' @param skill Numeric. Current skill value.
#' @param decay_rate Numeric. Decay rate per delivery.
#'
#' @return Numeric. Decayed skill value.
#' @keywords internal
apply_skill_decay <- function(skill, decay_rate = SKILL_DECAY_PER_DELIVERY) {
  skill * (1 - decay_rate)
}


#' Bound Skill to Valid Range
#'
#' Clips skill to soft bounds to prevent extreme values.
#'
#' @param skill Numeric. Current skill value.
#' @param skill_type Character. "run" or "wicket".
#' @param entity_type Character. "player" or "venue".
#'
#' @return Numeric. Bounded skill value.
#'
#' @examples
#' bound_skill(0.3, "run", "player")
#' bound_skill(0.8, "run", "player")  # Capped to max
#' bound_skill(0.01, "wicket", "venue")
#'
#' @export
bound_skill <- function(skill, skill_type = "run", entity_type = "player") {
  if (entity_type == "venue") {
    if (skill_type == "run") {
      return(pmax(SKILL_VENUE_RUN_MIN, pmin(SKILL_VENUE_RUN_MAX, skill)))
    } else {
      return(pmax(SKILL_VENUE_WICKET_MIN, pmin(SKILL_VENUE_WICKET_MAX, skill)))
    }
  } else {
    if (skill_type == "run") {
      return(pmax(SKILL_INDEX_RUN_MIN, pmin(SKILL_INDEX_RUN_MAX, skill)))
    } else {
      return(pmax(SKILL_INDEX_WICKET_MIN, pmin(SKILL_INDEX_WICKET_MAX, skill)))
    }
  }
}


#' Update Run Skills (All Entities)
#'
#' Calculates skill updates for the run dimension for all three entities.
#' Applies continuous decay first, then updates based on residual.
#'
#' Formula:
#'   new_skill = (1 - decay) * old_skill + alpha * residual * weight
#'
#' @param actual_runs Numeric. Actual runs scored on this delivery.
#' @param expected_runs Numeric. Expected runs from skill model.
#' @param alpha_batter Numeric. Batter's learning rate.
#' @param alpha_bowler Numeric. Bowler's learning rate.
#' @param alpha_venue_perm Numeric. Venue permanent learning rate.
#' @param alpha_venue_session Numeric. Venue session learning rate.
#' @param old_batter_skill Numeric. Batter's current run skill.
#' @param old_bowler_skill Numeric. Bowler's current run skill.
#' @param old_venue_perm_skill Numeric. Venue's permanent run skill.
#' @param old_venue_session_skill Numeric. Venue's session run skill.
#' @param format Character. Match format.
#' @param gender Character. Gender.
#'
#' @return List with new_batter, new_bowler, new_venue_perm, new_venue_session.
#' @keywords internal
update_run_skills <- function(actual_runs,
                               expected_runs,
                               alpha_batter,
                               alpha_bowler,
                               alpha_venue_perm,
                               alpha_venue_session,
                               old_batter_skill,
                               old_bowler_skill,
                               old_venue_perm_skill,
                               old_venue_session_skill,
                               format = "t20",
                               gender = "male") {

  weights <- get_skill_weights(format, gender, "run")
  decay <- get_skill_decay(format)

  # Calculate residual (actual - expected)
  residual <- actual_runs - expected_runs

  # Apply decay first, then update with residual
  # Batter: positive residual = scored well = skill increases
  new_batter <- apply_skill_decay(old_batter_skill, decay) +
    alpha_batter * residual * weights$w_batter

  # Bowler: inverted - if batter scored well, bowler skill DECREASES
  # Since bowler skill is "restricts runs", a good performance means negative residual
  new_bowler <- apply_skill_decay(old_bowler_skill, decay) -
    alpha_bowler * residual * weights$w_bowler

  # Venue permanent: high-scoring venue gets positive residual
  new_venue_perm <- apply_skill_decay(old_venue_perm_skill, SKILL_VENUE_DECAY_PERM) +
    alpha_venue_perm * residual * weights$w_venue_perm

  # Venue session: captures current conditions
  new_venue_session <- apply_skill_decay(old_venue_session_skill, SKILL_VENUE_DECAY_SESSION) +
    alpha_venue_session * residual * weights$w_venue_session

  # Apply bounds
  list(
    new_batter = bound_skill(new_batter, "run", "player"),
    new_bowler = bound_skill(new_bowler, "run", "player"),
    new_venue_perm = bound_skill(new_venue_perm, "run", "venue"),
    new_venue_session = bound_skill(new_venue_session, "run", "venue")
  )
}


#' Update Wicket Skills (All Entities)
#'
#' Calculates skill updates for the wicket dimension for all three entities.
#' Applies continuous decay first, then updates based on residual.
#'
#' @param actual_wicket Integer. 1 if wicket fell, 0 otherwise.
#' @param expected_wicket Numeric. Expected wicket probability.
#' @param alpha_batter Numeric. Batter's learning rate.
#' @param alpha_bowler Numeric. Bowler's learning rate.
#' @param alpha_venue_perm Numeric. Venue permanent learning rate.
#' @param alpha_venue_session Numeric. Venue session learning rate.
#' @param old_batter_skill Numeric. Batter's current wicket skill.
#' @param old_bowler_skill Numeric. Bowler's current wicket skill.
#' @param old_venue_perm_skill Numeric. Venue's permanent wicket skill.
#' @param old_venue_session_skill Numeric. Venue's session wicket skill.
#' @param format Character. Match format.
#' @param gender Character. Gender.
#'
#' @return List with new_batter, new_bowler, new_venue_perm, new_venue_session.
#' @keywords internal
update_wicket_skills <- function(actual_wicket,
                                  expected_wicket,
                                  alpha_batter,
                                  alpha_bowler,
                                  alpha_venue_perm,
                                  alpha_venue_session,
                                  old_batter_skill,
                                  old_bowler_skill,
                                  old_venue_perm_skill,
                                  old_venue_session_skill,
                                  format = "t20",
                                  gender = "male") {

  weights <- get_skill_weights(format, gender, "wicket")
  decay <- get_skill_decay(format)

  # Residual: actual_wicket - expected_wicket
  # Positive if wicket fell when unexpected (batter skill goes UP = gets out more)
  residual <- actual_wicket - expected_wicket

  # Batter: survived = negative residual = skill stays same or decreases
  #         got out = positive residual = skill increases (gets out more)
  # Note: Higher batter wicket skill = BAD for batter (gets out more)
  new_batter <- apply_skill_decay(old_batter_skill, decay) +
    alpha_batter * residual * weights$w_batter

  # Bowler: took wicket = positive residual = skill increases (takes more wickets)
  #         didn't take wicket = negative residual = skill decreases
  # Higher bowler wicket skill = GOOD for bowler (takes more wickets)
  new_bowler <- apply_skill_decay(old_bowler_skill, decay) +
    alpha_bowler * residual * weights$w_bowler

  # Venue: wicket-friendly = positive skill
  new_venue_perm <- apply_skill_decay(old_venue_perm_skill, SKILL_VENUE_DECAY_PERM) +
    alpha_venue_perm * residual * weights$w_venue_perm

  new_venue_session <- apply_skill_decay(old_venue_session_skill, SKILL_VENUE_DECAY_SESSION) +
    alpha_venue_session * residual * weights$w_venue_session

  # Apply bounds
  list(
    new_batter = bound_skill(new_batter, "wicket", "player"),
    new_bowler = bound_skill(new_bowler, "wicket", "player"),
    new_venue_perm = bound_skill(new_venue_perm, "wicket", "venue"),
    new_venue_session = bound_skill(new_venue_session, "wicket", "venue")
  )
}


# ============================================================================
# SESSION RESET
# ============================================================================

#' Reset Venue Session Skill for New Match
#'
#' Resets session skill to neutral (0) at the start of each match.
#'
#' @return Numeric. Starting session skill (0).
#' @keywords internal
reset_venue_session_skill <- function() {
  SKILL_INDEX_START
}


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

#' Build Skill Index Parameters for Format and Gender
#'
#' Constructs a parameter list for skill index calculation using
#' format-gender-specific parameters.
#'
#' @param format Character. Match format: "t20", "odi", or "test".
#' @param gender Character. Gender: "male" or "female". Default "male".
#'
#' @return List with all parameters needed for skill index calculation.
#'
#' @examples
#' build_skill_index_params("t20")
#' build_skill_index_params("test", gender = "female")
#'
#' @export
build_skill_index_params <- function(format = "t20", gender = "male") {
  format <- tolower(format)

  # Get format-gender-specific parameters
  run_alpha <- get_skill_alpha_params(format, gender, "run")
  wicket_alpha <- get_skill_alpha_params(format, gender, "wicket")
  run_weights <- get_skill_weights(format, gender, "run")
  wicket_weights <- get_skill_weights(format, gender, "wicket")

  # Alpha (learning rate) parameters
  alpha_params <- list(
    alpha_run_max = run_alpha$alpha_max,
    alpha_run_min = run_alpha$alpha_min,
    alpha_run_halflife = run_alpha$halflife,
    alpha_wicket_max = wicket_alpha$alpha_max,
    alpha_wicket_min = wicket_alpha$alpha_min,
    alpha_wicket_halflife = wicket_alpha$halflife
  )

  # Attribution weights
  weight_params <- list(
    w_batter_run = run_weights$w_batter,
    w_bowler_run = run_weights$w_bowler,
    w_venue_perm_run = run_weights$w_venue_perm,
    w_venue_session_run = run_weights$w_venue_session,
    w_batter_wicket = wicket_weights$w_batter,
    w_bowler_wicket = wicket_weights$w_bowler,
    w_venue_perm_wicket = wicket_weights$w_venue_perm,
    w_venue_session_wicket = wicket_weights$w_venue_session
  )

  # Decay and bounds
  decay_params <- list(
    decay_rate = get_skill_decay(format),
    venue_perm_decay = SKILL_VENUE_DECAY_PERM,
    venue_session_decay = SKILL_VENUE_DECAY_SESSION
  )

  bound_params <- list(
    skill_start = SKILL_INDEX_START,
    run_max = SKILL_INDEX_RUN_MAX,
    run_min = SKILL_INDEX_RUN_MIN,
    wicket_max = SKILL_INDEX_WICKET_MAX,
    wicket_min = SKILL_INDEX_WICKET_MIN,
    venue_run_max = SKILL_VENUE_RUN_MAX,
    venue_run_min = SKILL_VENUE_RUN_MIN,
    venue_wicket_max = SKILL_VENUE_WICKET_MAX,
    venue_wicket_min = SKILL_VENUE_WICKET_MIN
  )

  # Venue alpha parameters
  venue_alpha_params <- list(
    venue_alpha_perm = switch(format,
      "t20" = SKILL_VENUE_ALPHA_PERM_T20,
      "odi" = SKILL_VENUE_ALPHA_PERM_ODI,
      "test" = SKILL_VENUE_ALPHA_PERM_TEST,
      SKILL_VENUE_ALPHA_PERM_T20),
    venue_alpha_session_max = SKILL_VENUE_ALPHA_SESSION_MAX,
    venue_alpha_session_min = SKILL_VENUE_ALPHA_SESSION_MIN,
    venue_alpha_session_halflife = SKILL_VENUE_ALPHA_SESSION_HALFLIFE
  )

  # Common parameters
  common_params <- list(
    format = format,
    gender = gender
  )

  c(alpha_params, weight_params, decay_params, bound_params,
    venue_alpha_params, common_params)
}


#' Convert ELO to Skill Index (Approximate)
#'
#' Provides an approximate conversion from ELO ratings to skill indices
#' for migration or comparison purposes.
#'
#' The conversion uses the runs_per_100_elo factor to estimate the
#' run-equivalent skill value.
#'
#' Formula:
#'   skill = (elo - ELO_START) * runs_per_100_elo / 100
#'
#' Examples (Men's T20, runs_per_100_elo = 0.0745):
#'   ELO 1600 -> skill = (1600 - 1400) * 0.0745 / 100 = +0.149
#'   ELO 1200 -> skill = (1200 - 1400) * 0.0745 / 100 = -0.149
#'
#' @param elo Numeric. ELO rating.
#' @param format Character. Match format.
#' @param gender Character. Gender.
#' @param skill_type Character. "run" for runs, "wicket" for wickets.
#'
#' @return Numeric. Approximate skill index value.
#' @keywords internal
elo_to_skill <- function(elo, format = "t20", gender = "male", skill_type = "run") {
  elo_start <- THREE_WAY_ELO_START

  if (skill_type == "run") {
    runs_per_elo <- get_runs_per_100_elo(format, gender) / 100
    (elo - elo_start) * runs_per_elo
  } else {
    # For wickets, conversion is less direct
    # Approximate using divisor: each 400 ELO points ~= 1 logit unit
    divisor <- get_wicket_elo_divisor(format, gender)
    # Rough approximation: logit change per ELO point
    (elo - elo_start) / divisor * 0.05  # ~5% per 400 ELO
  }
}


#' Convert Skill Index to ELO (Approximate)
#'
#' Provides an approximate conversion from skill indices to ELO ratings
#' for comparison or migration purposes.
#'
#' @param skill Numeric. Skill index value.
#' @param format Character. Match format.
#' @param gender Character. Gender.
#' @param skill_type Character. "run" for runs, "wicket" for wickets.
#'
#' @return Numeric. Approximate ELO rating.
#' @keywords internal
skill_to_elo <- function(skill, format = "t20", gender = "male", skill_type = "run") {
  elo_start <- THREE_WAY_ELO_START

  if (skill_type == "run") {
    runs_per_elo <- get_runs_per_100_elo(format, gender) / 100
    elo_start + skill / runs_per_elo
  } else {
    divisor <- get_wicket_elo_divisor(format, gender)
    elo_start + skill * divisor / 0.05
  }
}


NULL
