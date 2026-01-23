# Test Coverage Analysis for Bouncer Package

## Executive Summary

The bouncer package currently has **8 test files** covering core functionality, but significant portions of the codebase remain untested. This analysis identifies coverage gaps and proposes high-priority areas for improvement.

**Current State:**
- **57 R source files** (~30,000+ lines of code)
- **8 test files** (~1,459 lines of tests)
- **Estimated coverage**: ~25-30% of exported functions

---

## Current Test Coverage

### Tested Modules

| Test File | Lines | Coverage Quality | Functions Tested |
|-----------|-------|------------------|------------------|
| `test-elo-core.R` | 111 | Good | `calculate_expected_outcome()`, `calculate_k_factor()`, `calculate_elo_update()`, `calculate_delivery_outcome_score()`, `initialize_player_elo()`, `normalize_match_type()` |
| `test-score-projection.R` | 393 | Excellent | `calculate_projection_resource()`, `calculate_projected_score()`, `get_agnostic_expected_score()`, `load_projection_params()`, `calculate_full_expected_score()`, `calculate_projection_change()`, vectorized functions, Test match helpers |
| `test-parser.R` | 252 | Good | `parse_cricsheet_json()` with wickets, extras, delivery ID formatting |
| `test-database.R` | 108 | Adequate | `find_bouncerdata_dir()`, `get_default_db_path()`, `get_db_connection()`, `initialize_bouncer_database()`, schema validation |
| `test-stat-functions.R` | 238 | Good | `player_batting_stats()`, `player_bowling_stats()`, `team_batting_stats()`, `team_bowling_stats()`, `head_to_head()`, `venue_stats()` |
| `test-team-predictions.R` | 179 | Good | `calculate_roster_elo()`, `compare_team_rosters()`, `predict_roster_matchup()`, `predict_innings_score()`, `simulate_match()` |
| `test-data-loaders.R` | 90 | Adequate | `check_duckdb_available()`, remote data functions, cache clearing |
| `test-skill-utils.R` | 88 | Good | `normalize_format()`, `get_skill_table_name()`, `escape_sql_strings()`, `batch_skill_query()` |

### Test Quality Observations

**Strengths:**
- ELO core functions have excellent boundary testing
- Score projection has comprehensive edge case coverage
- Database tests use proper skip conditions for CI environments
- SQL injection prevention is tested (`escape_sql_strings`)

**Weaknesses:**
- Many tests require database, limiting CI execution
- No tests for visualization output
- No tests for ML model predictions
- No simulation engine tests

---

## Untested Modules (Priority Order)

### Priority 1: Critical (High Impact, Core Functionality)

#### 1. `simulation.R` (~700 lines) - **UNTESTED**

Core match and season simulation engine. Tests needed for:

```r
# Functions to test:
- create_simulation_config()
- simulate_match_outcome()
- aggregate_match_results()
- aggregate_season_results()
- simulate_season()
- simulate_season_n()
- simulate_ipl_playoffs()
- elo_win_probability()
```

**Proposed Tests:**
- Verify `elo_win_probability()` matches ELO formula (symmetric, bounds)
- Test `simulate_match_outcome()` returns valid winner/loser
- Test `simulate_season()` produces valid standings (correct wins/losses/points)
- Test `aggregate_season_results()` calculates correct probabilities
- Test `simulate_ipl_playoffs()` follows correct bracket logic

#### 2. `in_match_prediction.R` (~585 lines) - **UNTESTED**

Live win probability predictions. Tests needed for:

```r
# Functions to test:
- predict_win_probability()
- get_default_venue_stats()
- add_win_probability()
```

**Proposed Tests:**
- Verify `predict_win_probability()` returns 0-1 range
- Test innings 1 vs innings 2 logic
- Test boundary conditions (start of innings, all out, target reached)
- Verify `get_default_venue_stats()` returns valid structure

#### 3. `elo_batting.R` / `elo_bowling.R` - **UNTESTED**

ELO update logic for batting and bowling. Tests needed for:

```r
# Functions to test:
- update_batting_elo()
- update_bowling_elo()
- process_delivery_elo()
```

**Proposed Tests:**
- Verify ELO updates are zero-sum (batter gain = bowler loss)
- Test updates for different delivery outcomes (wicket, dot, boundary)
- Test K-factor application by match type

### Priority 2: High (Important for Accuracy)

#### 4. `feature_engineering.R` (~509 lines) - **UNTESTED**

ML feature creation for predictions. Tests needed for:

```r
# Functions to test:
- calculate_rolling_features()
- calculate_phase_features()
- calculate_venue_statistics()
- calculate_pressure_metrics()
- calculate_run_rate()
- calculate_expected_runs_per_ball()
- calculate_era()
- calculate_tail_calibration_features()
- create_grouped_folds()
```

**Proposed Tests:**
- Test `calculate_phase_features()` returns correct phases for T20/ODI
- Test `calculate_pressure_metrics()` for edge cases (target reached, all out)
- Test `calculate_rolling_features()` produces expected values
- Test `create_grouped_folds()` keeps same match_id in same fold

#### 5. `delivery_simulation.R` - **UNTESTED**

Ball-by-ball simulation. Tests needed for:

```r
# Functions to test:
- simulate_delivery()
- simulate_innings()
- simulate_match_ballbyball()
```

**Proposed Tests:**
- Verify delivery outcomes are within valid ranges
- Test innings terminates at 10 wickets or max balls
- Test cumulative scoring logic

#### 6. `model_predictions.R` / `agnostic_model.R` - **UNTESTED**

ML model prediction interfaces. Tests needed for:

```r
# Functions to test:
- predict_with_model()
- load_model()
- get_model_features()
```

**Proposed Tests:**
- Test model loading error handling
- Verify predictions are within expected ranges
- Test fallback behavior when model unavailable

### Priority 3: Medium (User-Facing, Quality of Life)

#### 7. `visualization.R` (~683 lines) - **UNTESTED**

All plotting functions. Tests needed for:

```r
# Functions to test:
- theme_bouncer()
- plot_score_progression()
- plot_win_probability()
- plot_skill_progression()
- plot_player_comparison()
- plot_elo_history()
- plot_team_strength()
```

**Proposed Tests:**
- Verify functions return ggplot objects (not NULL)
- Test error handling when ggplot2 not available
- Test input validation
- Use `vdiffr` for visual regression testing

#### 8. `user_api.R` / `user_functions.R` - **PARTIALLY TESTED**

Public API functions. Additional tests needed for:

```r
# Functions to test:
- get_player()
- get_player_elo()
- rank_players()
- analyze_player()
- compare_players()
- get_team_elo()
```

**Proposed Tests:**
- Test player lookup with fuzzy matching
- Test error messages for invalid inputs
- Test output structure consistency

#### 9. `wpa_era.R` - **UNTESTED**

Win Probability Added and Expected Runs Added calculations.

**Proposed Tests:**
- Verify WPA sums to winner's gain
- Test ERA calculations match expected values
- Test attribution to batter/bowler

### Priority 4: Lower (Edge Cases, Infrastructure)

#### 10. `fox_scraper.R` - **UNTESTED**

External data scraping (network-dependent).

**Proposed Tests:**
- Mock HTTP responses for unit testing
- Test parsing of scraped data structures
- Test error handling for network failures

#### 11. `player_correlation.R` / `team_correlation.R` - **UNTESTED**

Correlation analysis functions.

**Proposed Tests:**
- Verify correlation values are in [-1, 1] range
- Test with edge cases (insufficient data)

#### 12. `margin_calculation.R` - **UNTESTED**

Match margin calculations.

**Proposed Tests:**
- Test run margin calculations
- Test wicket margin calculations
- Test DLS-adjusted margins

---

## Recommended Test Improvements

### 1. Add Unit Tests for Simulation Engine

**File:** `tests/testthat/test-simulation.R`

```r
test_that("elo_win_probability returns valid probabilities", {
  # Equal ELO = 50%
  expect_equal(elo_win_probability(1500, 1500), 0.5)

  # Higher ELO = higher probability
  expect_gt(elo_win_probability(1600, 1400), 0.5)

  # Symmetric property
  p1 <- elo_win_probability(1600, 1400)
  p2 <- elo_win_probability(1400, 1600)
  expect_equal(p1 + p2, 1)

  # Bounds
  expect_gte(elo_win_probability(2500, 1000), 0)
  expect_lte(elo_win_probability(2500, 1000), 1)
})

test_that("simulate_match_outcome returns valid structure", {
  result <- simulate_match_outcome(0.6, "Team A", "Team B")

  expect_type(result, "list")
  expect_true(result$winner %in% c("Team A", "Team B"))
  expect_true(result$loser %in% c("Team A", "Team B"))
  expect_false(result$winner == result$loser)
})

test_that("simulate_season produces valid standings", {
  fixtures <- data.frame(
    team1 = c("A", "B", "A"),
    team2 = c("B", "C", "C"),
    team1_win_prob = c(0.6, 0.5, 0.7)
  )

  standings <- simulate_season(fixtures)

  expect_s3_class(standings, "data.frame")
  expect_true(all(c("team", "wins", "losses", "points") %in% names(standings)))
  expect_equal(sum(standings$wins), nrow(fixtures))  # Total wins = total matches
})
```

### 2. Add Unit Tests for Win Probability

**File:** `tests/testthat/test-in-match-prediction.R`

```r
test_that("get_default_venue_stats returns expected structure", {
  stats <- get_default_venue_stats("t20")

  expect_type(stats, "list")
  expect_true("avg_first_innings" %in% names(stats))
  expect_true("chase_win_rate" %in% names(stats))
  expect_gt(stats$avg_first_innings, 100)
  expect_lt(stats$avg_first_innings, 250)
})

test_that("predict_win_probability validates innings parameter", {
  expect_error(
    predict_win_probability(50, 2, 10, innings = 3, format = "t20"),
    "innings must be 1 or 2"
  )
})

test_that("predict_win_probability requires target for 2nd innings", {
  expect_error(
    predict_win_probability(50, 2, 10, innings = 2, format = "t20"),
    "Target is required"
  )
})
```

### 3. Add Unit Tests for Feature Engineering

**File:** `tests/testthat/test-feature-engineering.R`

```r
test_that("calculate_phase_features identifies T20 phases correctly", {
  # Powerplay (overs 0-5)
  pp <- calculate_phase_features(over = 3, ball = 1, match_type = "t20")
  expect_equal(as.character(pp$phase), "powerplay")

  # Middle overs (6-15)
  mid <- calculate_phase_features(over = 10, ball = 1, match_type = "t20")
  expect_equal(as.character(mid$phase), "middle")

  # Death overs (16-19)
  death <- calculate_phase_features(over = 18, ball = 1, match_type = "t20")
  expect_equal(as.character(death$phase), "death")
})

test_that("calculate_pressure_metrics handles target reached", {
  metrics <- calculate_pressure_metrics(
    target = 150,
    current_runs = 155,
    current_wickets = 3,
    balls_remaining = 30,
    current_run_rate = 9.0
  )

  expect_equal(metrics$runs_needed, 0)
  expect_equal(metrics$required_run_rate, 0)
})

test_that("calculate_run_rate handles zero balls", {
  expect_equal(calculate_run_rate(0, 0), 0)
})

test_that("create_grouped_folds keeps matches together", {
  data <- data.frame(
    match_id = rep(c("m1", "m2", "m3", "m4", "m5"), each = 10),
    value = 1:50
  )

  folds <- create_grouped_folds(data, n_folds = 2, seed = 42)

  # Check each fold has complete matches
  for (fold_indices in folds) {
    fold_data <- data[fold_indices, ]
    matches_in_fold <- unique(fold_data$match_id)

    for (m in matches_in_fold) {
      # All rows for this match should be in this fold
      expect_equal(
        sum(fold_data$match_id == m),
        sum(data$match_id == m)
      )
    }
  }
})
```

### 4. Add Visualization Tests

**File:** `tests/testthat/test-visualization.R`

```r
test_that("theme_bouncer returns ggplot theme", {
  skip_if_not_installed("ggplot2")

  theme <- theme_bouncer()
  expect_s3_class(theme, "theme")
})

test_that("theme_bouncer errors without ggplot2", {
  # This test would require mocking - skip for now
  skip("Requires package mocking")
})

test_that("plot functions handle missing data gracefully", {
  skip_if_not_installed("ggplot2")
  skip_if_not(file.exists(get_default_db_path()), "Database not available")

  # Test with non-existent match
  result <- plot_score_progression("non_existent_match_12345")
  expect_null(result)
})
```

---

## Testing Infrastructure Improvements

### 1. Create Test Fixtures

Create reusable test data in `tests/testthat/fixtures/`:

```r
# tests/testthat/fixtures/sample_deliveries.R
sample_deliveries <- data.frame(
  match_id = "test_match_001",
  innings = c(rep(1, 6), rep(2, 6)),
  over = c(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
  ball = rep(1:6, 2),
  runs_total = c(1, 0, 4, 2, 6, 1, 0, 1, 4, 0, 1, 2),
  is_wicket = c(FALSE, TRUE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, TRUE, FALSE, FALSE),
  is_four = c(FALSE, FALSE, TRUE, FALSE, FALSE, FALSE, FALSE, FALSE, TRUE, FALSE, FALSE, FALSE),
  is_six = c(FALSE, FALSE, FALSE, FALSE, TRUE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE)
)
```

### 2. Add Test Helper Functions

Create `tests/testthat/helper-functions.R`:

```r
# Skip test if database not available
skip_if_no_db <- function() {
  db_path <- tryCatch(get_db_path(), error = function(e) NULL)
  if (is.null(db_path) || !file.exists(db_path)) {
    skip("Database not available")
  }
}

# Create temporary test database
with_test_db <- function(code) {
  temp_db <- tempfile(fileext = ".duckdb")
  on.exit(unlink(temp_db), add = TRUE)
  initialize_bouncer_database(path = temp_db, overwrite = TRUE)
  code
}
```

### 3. Set Up CI Testing

Ensure GitHub Actions workflow runs tests:

```yaml
# .github/workflows/R-CMD-check.yaml
- name: Run tests
  run: |
    testthat::test_local(reporter = "summary")
  shell: Rscript {0}
```

---

## Metrics & Goals

### Current Metrics (Estimated)

| Metric | Value |
|--------|-------|
| Test files | 8 |
| Test lines | ~1,459 |
| Source files | 57 |
| Exported functions | 105 |
| Functions with tests | ~35 (33%) |

### Target Metrics

| Metric | Current | Target | Priority |
|--------|---------|--------|----------|
| Functions with tests | 33% | 70% | High |
| Simulation coverage | 0% | 80% | Critical |
| Prediction coverage | 20% | 80% | Critical |
| Feature eng. coverage | 0% | 70% | High |
| Visualization coverage | 0% | 50% | Medium |

---

## Implementation Roadmap

### Phase 1 (Immediate - 1-2 weeks)
1. Add simulation engine tests (`test-simulation.R`)
2. Add in-match prediction tests (`test-in-match-prediction.R`)
3. Add feature engineering tests (`test-feature-engineering.R`)

### Phase 2 (Short-term - 3-4 weeks)
1. Add ELO batting/bowling tests
2. Add delivery simulation tests
3. Improve test fixtures and helpers

### Phase 3 (Medium-term - 1-2 months)
1. Add visualization tests with `vdiffr`
2. Add integration tests for full match processing
3. Set up code coverage reporting

### Phase 4 (Ongoing)
1. Maintain 70%+ coverage for new code
2. Add regression tests for bug fixes
3. Performance benchmarking tests

---

## Conclusion

The bouncer package has a solid foundation of tests for core ELO calculations and score projection, but significant gaps exist in:

1. **Simulation engine** - Critical for season predictions
2. **Win probability** - Core user-facing feature
3. **Feature engineering** - Affects model accuracy
4. **Visualization** - User experience

Addressing Priority 1 and 2 items will significantly improve package reliability and catch regressions early. The proposed test implementations provide concrete starting points for each module.
