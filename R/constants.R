# Package Constants for Bouncer
#
# Main entry point for package constants.
#
# Domain-specific constants are organized in separate files:
#   - constants_elo.R: Basic ELO ratings, dual ELO, dynamic K-factors
#   - constants_skill.R: Player/venue skill indices, format mappings
#   - constants_projection.R: Score projection parameters
#   - constants_centrality.R: Network centrality, margin of victory
#   - constants_3way.R: 3-Way ELO/Glicko system constants
#   - globals-core.R: All globalVariables() declarations
#
# This file contains only shared data organization categories.

# ============================================================================
# DATA ORGANIZATION FORMAT CATEGORIES
# ============================================================================
# For organizing data into folders and releases
# Uses long_form/short_form instead of test/odi/t20

# Long form = day-limited matches (Tests, First-class, multi-day)
FORMAT_LONG_FORM <- c("Test", "MDM")

# Short form = over/ball-limited matches (everything else)
FORMAT_SHORT_FORM <- c("ODI", "ODM", "T20", "IT20", "T10")

# International match types (for international vs club classification)
MATCH_TYPE_INTERNATIONAL <- c("Test", "ODI", "IT20")

# All data partition folders (match_type x gender x team_type)
# Based on actual partitions created by daily scraper
DATA_FOLDERS <- c(
  # Test format
  "Test_male_international",
  "Test_female_international",
  # ODI format
  "ODI_male_international",
  "ODI_female_international",
  # T20 format (includes franchise leagues)
  "T20_male_international",
  "T20_male_club",
  "T20_female_international",
  "T20_female_club",
  # IT20 (domestic T20 internationals)
  "IT20_male_international",
  "IT20_female_international",
  # MDM (multi-day matches / first-class)
  "MDM_male_international",
  "MDM_male_club",
  "MDM_female_international",
  "MDM_female_club",
  # ODM (domestic one-day)
  "ODM_male_international",
  "ODM_male_club"
)

NULL
