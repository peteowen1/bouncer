# Package Constants for Bouncer

#' ELO Rating Constants
#'
#' @keywords internal
#' @name elo_constants

# Starting ELO rating for all players
ELO_START_RATING <- 1500

# K-factors by match type (learning rate)
K_FACTOR_TEST <- 32
K_FACTOR_ODI <- 24
K_FACTOR_T20 <- 20
K_FACTOR_DOMESTIC <- 16

# Average runs per ball by format (used in predictions)
AVG_RUNS_PER_BALL_TEST <- 3.0
AVG_RUNS_PER_BALL_ODI <- 5.0
AVG_RUNS_PER_BALL_T20 <- 7.5

# Base wicket probability per delivery by format
BASE_WICKET_PROB_TEST <- 0.035   # ~1 wicket per 30 balls
BASE_WICKET_PROB_ODI <- 0.030    # ~1 wicket per 33 balls
BASE_WICKET_PROB_T20 <- 0.025    # ~1 wicket per 40 balls

# ELO calculation constant (standard chess-style)
ELO_DIVISOR <- 400

# Match type mappings
MATCH_TYPE_MAP <- c(
  "Test" = "test",
  "ODI" = "odi",
  "T20" = "t20",
  "T20I" = "t20",
  "MDM" = "odi",  # Men's Domestic Match
  "IT20" = "t20"
)

# Format classification
FORMAT_TEST <- c("test", "Test")
FORMAT_ODI <- c("odi", "ODI", "MDM")
FORMAT_T20 <- c("t20", "T20", "T20I", "IT20")

NULL
