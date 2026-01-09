# Calculate Per-Delivery Score Projections
#
# This script calculates and stores score projections for every delivery
# in the database, using the optimized parameters from step 01.
#
# Prerequisites:
#   - Run 01_optimize_projection_params.R first to generate parameter files
#   - Parameter files should exist in ../bouncerdata/models/
#
# Usage:
#   source("data-raw/ratings/projection/02_calculate_projections.R")

library(cli)

devtools::load_all()

# Calculate projections for all formats
calculate_all_format_projections(
  db_path = "../bouncerdata/bouncer.duckdb",
  formats = c("t20", "odi", "test"),
  batch_size = 50000
)
