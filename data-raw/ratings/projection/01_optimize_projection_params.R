# Optimize Score Projection Parameters
#
# This script optimizes the projection parameters (a, b, z, y) for each
# segment (format x gender x team_type) by minimizing RMSE against actual
# final innings totals.
#
# Usage:
#   source("data-raw/ratings/projection/01_optimize_projection_params.R")

library(dplyr)
library(cli)

devtools::load_all()

# Run optimization for all segments
results <- optimize_all_projection_segments(
  db_path = "../bouncerdata/bouncer.duckdb",
  output_dir = "../bouncerdata/models",
  sample_frac = 0.5
)
