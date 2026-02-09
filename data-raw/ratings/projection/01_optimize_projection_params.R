# Optimize Score Projection Parameters ----
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

# Run Optimization ----

results <- optimize_all_projection_segments(
  db_path = get_db_path(),
  output_dir = file.path(find_bouncerdata_dir(), "models"),
  sample_frac = 0.5
)
