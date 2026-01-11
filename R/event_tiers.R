# Event Tier System for League-Normalized Team ELO
#
# Classifies cricket events into quality tiers to:
# 1. Set appropriate starting ELO for new teams
# 2. Boost K-factor for cross-tier matches (more informative games)
#
# Tiers:
#   1 = Elite (major franchise leagues, ICC World events)
#   2 = Strong (national domestic leagues, good franchises)
#   3 = Regional (ICC qualifiers, associate tournaments)
#   4 = Development (small regional events, invitational)

# ============================================================================
# TIER STARTING ELOS
# ============================================================================

#' @keywords internal
TIER_1_STARTING_ELO <- 1550
TIER_2_STARTING_ELO <- 1500
TIER_3_STARTING_ELO <- 1400
TIER_4_STARTING_ELO <- 1300

# ============================================================================
# CROSS-TIER K-FACTOR BOOST
# ============================================================================

#' @keywords internal
CROSS_TIER_K_BOOST <- 0.5          # K multiplier per tier difference
CROSS_TIER_K_MAX_MULTIPLIER <- 2.5 # Maximum K multiplier

# ============================================================================
# TEAM TIER TRACKING
# ============================================================================

#' @keywords internal
TEAM_TIER_EMA_ALPHA <- 0.2  # Weight for most recent event in EMA

# ============================================================================
# EVENT TIER MAPPINGS
# ============================================================================

# Tier 1 - Elite (major franchise leagues and ICC World events)
TIER_1_EVENTS <- c(
 "Indian Premier League",
 "Big Bash League",
 "Pakistan Super League",
 "Caribbean Premier League",
 "SA20",
 "The Hundred Men's Competition",
 "The Hundred Women's Competition",
 "International League T20",
 "Major League Cricket",
 "ICC Men's T20 World Cup",
 "ICC Women's T20 World Cup",
 "ICC World Twenty20",
 "World T20",
 "Women's World T20"
)

# Tier 2 - Strong (strong domestic leagues)
TIER_2_EVENTS <- c(
 "Vitality Blast",
 "Vitality Blast Men",
 "Vitality Blast Women",
 "NatWest T20 Blast",
 "Syed Mushtaq Ali Trophy",
 "Women's Big Bash League",
 "Women's Premier League",
 "Super Smash",
 "Women's Super Smash",
 "Bangladesh Premier League",
 "Lanka Premier League",
 "Nepal Premier League",
 "Mzansi Super League",
 "CSA T20 Challenge",
 "Ram Slam T20 Challenge",
 "MiWAY T20 Challenge",
 "Women's Cricket Super League",
 "Charlotte Edwards Cup",
 "Women's Caribbean Premier League",
 "FairBreak Invitational Tournament",
 "Asia Cup",
 "Women's Asia Cup"
)

# Tier 3 patterns - Regional/Qualifiers (match with patterns)
TIER_3_PATTERNS <- c(
 "Qualifier",
 "Region",
 "Continental",
 "ACC ",
 "Asian Cricket Council"
)

# Tier 4 - Development (small events)
TIER_4_EVENTS <- c(
 "Kwibuka",
 "Valletta Cup",
 "Mdina Cup"
)

# ============================================================================
# TIER LOOKUP FUNCTIONS
# ============================================================================

#' Get Event Tier
#'
#' Determines the tier (1-4) for a given event based on event name.
#' Uses exact matching for known events and pattern matching for qualifiers.
#'
#' @param event_name Character. The event/tournament name.
#' @return Integer. Tier level (1-4), with 3 as default for unknown events.
#' @keywords internal
get_event_tier <- function(event_name) {
 if (is.null(event_name) || is.na(event_name) || event_name == "") {
   return(3L)  # Default to regional for unknown
 }

 # Check Tier 1 (exact match)
 if (event_name %in% TIER_1_EVENTS) {
   return(1L)
 }

 # Check Tier 2 (exact match)
 if (event_name %in% TIER_2_EVENTS) {
   return(2L)
 }

 # Check Tier 4 (exact match first, before pattern matching)
 if (event_name %in% TIER_4_EVENTS) {
   return(4L)
 }

 # Check for Tier 4 patterns (Kwibuka is a pattern)
 if (grepl("Kwibuka", event_name, ignore.case = TRUE)) {
   return(4L)
 }

 # Check Tier 3 patterns
 for (pattern in TIER_3_PATTERNS) {
   if (grepl(pattern, event_name, ignore.case = TRUE)) {
     return(3L)
   }
 }

 # Also check for common qualifier/regional indicators
 if (grepl("Africa|Americas|Europe|Asia|Pacific|EAP", event_name, ignore.case = TRUE)) {
   # But exclude elite events that contain these words
   if (!grepl("SA20|Hundred|Blast|Premier League", event_name, ignore.case = TRUE)) {
     return(3L)
   }
 }

 # Default to Tier 3 (conservative)
 return(3L)
}

#' Get Tier Starting ELO
#'
#' Returns the starting ELO for new teams based on the event tier.
#'
#' @param event_name Character. The event/tournament name.
#' @return Numeric. Starting ELO for the tier.
#' @keywords internal
get_tier_starting_elo <- function(event_name) {
 tier <- get_event_tier(event_name)
 switch(tier,
   TIER_1_STARTING_ELO,
   TIER_2_STARTING_ELO,
   TIER_3_STARTING_ELO,
   TIER_4_STARTING_ELO
 )
}

#' Get Cross-Tier K-Factor Multiplier
#'
#' Calculates the K-factor multiplier for matches between teams from
#' different tiers. Cross-tier matches are more informative for
#' calibrating ratings, so they get higher K-factors.
#'
#' @param tier1 Integer. Tier of team 1 (1-4).
#' @param tier2 Integer. Tier of team 2 (1-4).
#' @param boost_per_tier Numeric. K-factor boost per tier difference.
#'   Default uses package constant CROSS_TIER_K_BOOST (0.5).
#' @param max_multiplier Numeric. Maximum K multiplier.
#'   Default uses package constant CROSS_TIER_K_MAX_MULTIPLIER (2.5).
#' @return Numeric. K-factor multiplier (1.0 for same tier, up to max_multiplier).
#' @keywords internal
get_cross_tier_k_multiplier <- function(tier1, tier2,
                                         boost_per_tier = CROSS_TIER_K_BOOST,
                                         max_multiplier = CROSS_TIER_K_MAX_MULTIPLIER) {
 tier_diff <- abs(tier1 - tier2)
 if (tier_diff == 0) {
   return(1.0)
 }

 # Linear scaling based on tier difference
 multiplier <- 1.0 + (tier_diff * boost_per_tier)
 min(multiplier, max_multiplier)
}

#' Get Player Cross-Tier K-Factor Multiplier
#'
#' Calculates the K-factor multiplier for player matchups based on team tiers.
#' When a player from a lower-tier team faces a player from a higher-tier team,
#' the K-factor is boosted to speed calibration.
#'
#' This uses the same principle as team ELO: cross-tier matches are more
#' informative for determining true skill levels.
#'
#' @param batter_team_tier Integer. Tier of batter's team (1-4).
#' @param bowler_team_tier Integer. Tier of bowler's team (1-4).
#' @param boost_per_tier Numeric. K-factor boost per tier difference (default 0.3).
#' @param max_multiplier Numeric. Maximum K multiplier (default 2.0).
#' @return Numeric. K-factor multiplier (1.0 for same tier, up to max_multiplier).
#' @keywords internal
get_player_cross_tier_k_multiplier <- function(batter_team_tier,
                                                bowler_team_tier,
                                                boost_per_tier = 0.3,
                                                max_multiplier = 2.0) {
  # Handle NA/NULL
  if (is.null(batter_team_tier) || is.na(batter_team_tier) ||
      is.null(bowler_team_tier) || is.na(bowler_team_tier)) {
    return(1.0)
  }

  tier_diff <- abs(batter_team_tier - bowler_team_tier)
  if (tier_diff == 0) {
    return(1.0)
  }

  # Linear scaling with player-specific boost
  multiplier <- 1.0 + (tier_diff * boost_per_tier)
  min(multiplier, max_multiplier)
}

#' Update Team Primary Tier (EMA)
#'
#' Updates a team's primary tier using exponential moving average.
#' This tracks which tier of events a team typically plays in.
#'
#' @param current_tier Numeric. Current primary tier (can be fractional).
#' @param event_tier Integer. Tier of the current event.
#' @param alpha Numeric. EMA decay factor (default from constant).
#' @return Numeric. Updated primary tier.
#' @keywords internal
update_team_tier <- function(current_tier, event_tier, alpha = TEAM_TIER_EMA_ALPHA) {
 if (is.null(current_tier) || is.na(current_tier)) {
   return(as.numeric(event_tier))
 }
 alpha * event_tier + (1 - alpha) * current_tier
}

#' Get Team Primary Tier (Rounded)
#'
#' Returns the team's primary tier as an integer (rounded from EMA).
#'
#' @param tier_ema Numeric. The EMA tier value.
#' @return Integer. Rounded tier (1-4).
#' @keywords internal
get_team_tier_rounded <- function(tier_ema) {
 if (is.null(tier_ema) || is.na(tier_ema)) {
   return(3L)  # Default
 }
 as.integer(round(tier_ema))
}

#' Get Tier Label
#'
#' Returns a human-readable label for a tier.
#'
#' @param tier Integer. Tier level (1-4).
#' @return Character. Tier label.
#' @keywords internal
get_tier_label <- function(tier) {
 switch(tier,
   "Elite",
   "Strong",
   "Regional",
   "Development"
 )
}

NULL
