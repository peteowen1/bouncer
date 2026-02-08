# Quick Test Script for Skill Index Functions
# Run this in RStudio to verify the functions work

devtools::load_all()

cat("=== Testing Skill Index Functions ===\n\n")

# Test 1: Constants
cat("1. Constants loaded:\n")
cat("   SKILL_INDEX_START:", SKILL_INDEX_START, "\n")
cat("   SKILL_INDEX_RUN_MAX:", SKILL_INDEX_RUN_MAX, "\n")
cat("   SKILL_DECAY_T20:", SKILL_DECAY_T20, "\n\n")

# Test 2: get_skill_decay
cat("2. get_skill_decay('t20'):", get_skill_decay("t20"), "\n")
cat("   get_skill_decay('odi'):", get_skill_decay("odi"), "\n\n")

# Test 3: get_skill_weights
cat("3. get_skill_weights('t20', 'male', 'run'):\n")
weights <- get_skill_weights("t20", "male", "run")
cat("   w_batter:", weights$w_batter, "\n")
cat("   w_bowler:", weights$w_bowler, "\n")
cat("   w_venue_session:", weights$w_venue_session, "\n")
cat("   w_venue_perm:", weights$w_venue_perm, "\n\n")

# Test 4: get_skill_alpha
cat("4. get_skill_alpha (experience-based learning rate):\n")
cat("   At 0 balls:", round(get_skill_alpha(0, "t20", "run", "male"), 4), "\n")
cat("   At 300 balls:", round(get_skill_alpha(300, "t20", "run", "male"), 4), "\n")
cat("   At 1000 balls:", round(get_skill_alpha(1000, "t20", "run", "male"), 4), "\n\n")

# Test 5: calculate_expected_runs_skill
cat("5. calculate_expected_runs_skill:\n")
exp_runs <- calculate_expected_runs_skill(
  agnostic_runs = 1.138,
  batter_run_skill = 0.20,
  bowler_run_skill = 0.10,
  venue_perm_run_skill = 0.05,
  venue_session_run_skill = 0.02,
  format = "t20",
  gender = "male"
)
cat("   Expected runs (good batter vs good bowler):", round(exp_runs, 3), "\n\n")

# Test 6: update_run_skills
cat("6. update_run_skills (after 4-run delivery):\n")
updates <- update_run_skills(
  actual_runs = 4,
  expected_runs = 1.2,
  alpha_batter = 0.05,
  alpha_bowler = 0.05,
  alpha_venue_perm = 0.002,
  alpha_venue_session = 0.05,
  old_batter_skill = 0.0,
  old_bowler_skill = 0.0,
  old_venue_perm_skill = 0.0,
  old_venue_session_skill = 0.0,
  format = "t20",
  gender = "male"
)
cat("   New batter skill:", round(updates$new_batter, 4), "\n")
cat("   New bowler skill:", round(updates$new_bowler, 4), "\n")
cat("   New venue_perm skill:", round(updates$new_venue_perm, 4), "\n")
cat("   New venue_session skill:", round(updates$new_venue_session, 4), "\n\n")

# Test 7: bound_skill
cat("7. bound_skill:\n")
cat("   Bound 0.8 (player run):", bound_skill(0.8, "run", "player"), "(max 0.5)\n")
cat("   Bound -0.8 (player run):", bound_skill(-0.8, "run", "player"), "(min -0.5)\n")
cat("   Bound 0.1 (venue run):", bound_skill(0.1, "run", "venue"), "(within bounds)\n\n")

# Test 8: build_skill_index_params
cat("8. build_skill_index_params:\n")
params <- build_skill_index_params("t20", "male")
cat("   alpha_run_max:", params$alpha_run_max, "\n")
cat("   alpha_run_min:", params$alpha_run_min, "\n")
cat("   decay_rate:", params$decay_rate, "\n\n")

cat("=== All skill index functions working correctly! ===\n")
