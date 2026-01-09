# Debug script to test new team ELO schema
# Run from bouncer/ directory

library(cli)
devtools::load_all()

cli::cli_h1("Testing Team ELO with New Schema")

# Source the team ELO calculation script
source("data-raw/ratings/team/01_calculate_team_elos.R")
