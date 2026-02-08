# Summarize all optimized 3-way ELO parameters
# across all format-gender combinations

library(dplyr)
library(tidyr)
library(cli)
devtools::load_all()

models_dir <- file.path(find_bouncerdata_dir(), "models")

# Define all expected files
params_files <- list(
  run = list(
    mens_t20 = "run_elo_params_mens_t20.rds",
    mens_odi = "run_elo_params_mens_odi.rds",
    mens_test = "run_elo_params_mens_test.rds",
    womens_t20 = "run_elo_params_womens_t20.rds",
    womens_odi = "run_elo_params_womens_odi.rds",
    womens_test = "run_elo_params_womens_test.rds"
  ),
  wicket = list(
    mens_t20 = "wicket_elo_params_mens_t20.rds",
    mens_odi = "wicket_elo_params_mens_odi.rds",
    mens_test = "wicket_elo_params_mens_test.rds",
    womens_t20 = "wicket_elo_params_womens_t20.rds",
    womens_odi = "wicket_elo_params_womens_odi.rds",
    womens_test = "wicket_elo_params_womens_test.rds"
  )
)

# Load and extract optimized_params from each file
load_params <- function(file_path, type, format_gender) {
  if (!file.exists(file_path)) {
    return(NULL)
  }

  result <- readRDS(file_path)
  params <- result$optimized_params

  data.frame(
    type = type,
    format_gender = format_gender,
    param = names(params),
    value = unname(params)
  )
}

# Load all run params
run_params <- lapply(names(params_files$run), function(fg) {
  file_path <- file.path(models_dir, params_files$run[[fg]])
  load_params(file_path, "run", fg)
}) |> bind_rows()

# Load all wicket params
wicket_params <- lapply(names(params_files$wicket), function(fg) {
  file_path <- file.path(models_dir, params_files$wicket[[fg]])
  load_params(file_path, "wicket", fg)
}) |> bind_rows()

all_params <- bind_rows(run_params, wicket_params)

# Pivot to wide format for comparison
cat("\n\n=== RUN ELO PARAMETERS (Poisson Loss) ===\n\n")
run_wide <- run_params |>
  pivot_wider(names_from = format_gender, values_from = value)
print(run_wide, n = 20)

cat("\n\n=== WICKET ELO PARAMETERS (Log-Loss) ===\n\n")
wicket_wide <- wicket_params |>
  pivot_wider(names_from = format_gender, values_from = value)
print(wicket_wide, n = 20)

# Focus on key attribution weights
cat("\n\n=== KEY ATTRIBUTION WEIGHTS COMPARISON ===\n")
cat("\nRun ELO (w_batter, w_bowler, w_venue = 1 - w_batter - w_bowler):\n")

key_run <- run_params |>
  filter(param %in% c("w_batter", "w_bowler")) |>
  pivot_wider(names_from = format_gender, values_from = value)
print(key_run)

# Calculate venue weight
cat("\nImplied w_venue (run):\n")
run_venue <- run_params |>
  filter(param %in% c("w_batter", "w_bowler")) |>
  pivot_wider(names_from = param, values_from = value) |>
  mutate(w_venue = round(1 - w_batter - w_bowler, 3)) |>
  select(format_gender, w_venue)
print(run_venue)

cat("\nWicket ELO (w_batter, w_bowler):\n")
key_wicket <- wicket_params |>
  filter(param %in% c("w_batter", "w_bowler")) |>
  pivot_wider(names_from = format_gender, values_from = value)
print(key_wicket)
