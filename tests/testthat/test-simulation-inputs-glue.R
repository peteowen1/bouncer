# bouncerverse#66: nothing turned "team A vs team B at venue V" into the
# argument shapes simulate_match_ballbyball() reads. build_match_simulation_
# inputs() (R/simulation_inputs.R) is that glue. These tests use an
# in-memory DuckDB fixture -- the real bouncer.duckdb is locked by a
# long-running job while this was written.

make_fixture <- function(conn, format = "t20") {
  DBI::dbExecute(conn, "CREATE SCHEMA IF NOT EXISTS cricsheet")

  DBI::dbExecute(conn, "CREATE TABLE cricsheet.players (
    player_id VARCHAR, player_name VARCHAR)")
  DBI::dbExecute(conn, "CREATE TABLE cricsheet.matches (
    match_id VARCHAR, team1 VARCHAR, team2 VARCHAR, venue VARCHAR,
    match_type VARCHAR, gender VARCHAR, team_type VARCHAR)")
  DBI::dbExecute(conn, "CREATE TABLE main.match_squads (
    match_id VARCHAR, team VARCHAR, player_id VARCHAR, player_name VARCHAR,
    from_registry BOOLEAN)")

  tbl <- paste0(format, "_player_skill")
  DBI::dbExecute(conn, sprintf("CREATE TABLE %s (
    delivery_id VARCHAR, match_id VARCHAR, match_date DATE,
    batter_id VARCHAR, bowler_id VARCHAR,
    batter_scoring_index DOUBLE, batter_survival_rate DOUBLE,
    bowler_economy_index DOUBLE, bowler_strike_rate DOUBLE)", tbl))

  team_tbl <- paste0(format, "_team_skill")
  DBI::dbExecute(conn, sprintf("CREATE TABLE %s (
    delivery_id VARCHAR, match_id VARCHAR, match_date DATE,
    batting_team_id VARCHAR, bowling_team_id VARCHAR,
    batting_team_runs_skill DOUBLE, batting_team_wicket_skill DOUBLE,
    bowling_team_runs_skill DOUBLE, bowling_team_wicket_skill DOUBLE,
    batting_team_balls INTEGER, bowling_team_balls INTEGER)", team_tbl))

  venue_tbl <- paste0(format, "_venue_skill")
  DBI::dbExecute(conn, sprintf("CREATE TABLE %s (
    delivery_id VARCHAR, match_id VARCHAR, match_date DATE, venue VARCHAR,
    venue_run_rate DOUBLE, venue_wicket_rate DOUBLE,
    venue_boundary_rate DOUBLE, venue_dot_rate DOUBLE, venue_balls INTEGER)",
    venue_tbl))

  invisible(list(player_skill = tbl, team_skill = team_tbl, venue_skill = venue_tbl))
}

insert_player_skill <- function(conn, format, batter_id, bowler_id,
                                 batter_scoring_index = NA, batter_survival_rate = NA,
                                 bowler_economy_index = NA, bowler_strike_rate = NA,
                                 match_date = "2024-01-01", delivery_id = NULL) {
  delivery_id <- delivery_id %||% paste0("d_", batter_id, "_", bowler_id)
  DBI::dbExecute(conn, sprintf("INSERT INTO %s_player_skill VALUES
    ('%s', 'm1', '%s', '%s', '%s', %s, %s, %s, %s)",
    format, delivery_id, match_date, batter_id, bowler_id,
    ifelse(is.na(batter_scoring_index), "NULL", batter_scoring_index),
    ifelse(is.na(batter_survival_rate), "NULL", batter_survival_rate),
    ifelse(is.na(bowler_economy_index), "NULL", bowler_economy_index),
    ifelse(is.na(bowler_strike_rate), "NULL", bowler_strike_rate)))
}

insert_team_skill <- function(conn, format, batting_team_id, bowling_team_id,
                               batting_runs, batting_wicket, bowling_runs, bowling_wicket,
                               match_date = "2024-01-01", delivery_id = "td1") {
  DBI::dbExecute(conn, sprintf("INSERT INTO %s_team_skill VALUES
    ('%s', 'm1', '%s', '%s', '%s', %s, %s, %s, %s, 10, 10)",
    format, delivery_id, match_date, batting_team_id, bowling_team_id,
    batting_runs, batting_wicket, bowling_runs, bowling_wicket))
}

insert_venue_skill <- function(conn, format, venue, run_rate, wicket_rate,
                                boundary_rate, dot_rate,
                                match_date = "2024-01-01", delivery_id = "vd1") {
  DBI::dbExecute(conn, sprintf("INSERT INTO %s_venue_skill VALUES
    ('%s', 'm1', '%s', '%s', %s, %s, %s, %s, 100)",
    format, delivery_id, match_date, venue, run_rate, wicket_rate, boundary_rate, dot_rate))
}

test_that("field names reaching simulate_match_ballbyball are exact, and the real XI is preferred", {
  conn <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
  make_fixture(conn, "t20")

  DBI::dbExecute(conn, "INSERT INTO cricsheet.matches VALUES
    ('m1', 'India', 'Australia', 'MCG', 'T20', 'male', 'international')")
  DBI::dbExecute(conn, "INSERT INTO cricsheet.players VALUES
    ('p1', 'Player One'), ('p2', 'Player Two'), ('a1', 'Aussie One'), ('a2', 'Aussie Two')")
  DBI::dbExecute(conn, "INSERT INTO main.match_squads VALUES
    ('m1', 'India', 'p1', 'Player One', TRUE),
    ('m1', 'India', 'p2', 'Player Two', TRUE),
    ('m1', 'Australia', 'a1', 'Aussie One', TRUE),
    ('m1', 'Australia', 'a2', 'Aussie Two', TRUE)")

  # d1: p1 bats, a1 bowls. a1's bowling values (5.0/0.99) sit in the SAME row
  # as p1's batting values -- deliberately extreme, so if the code ever read
  # bowler_* off a row keyed by batter_id = p1, it would show up as p1's own
  # bowling skill and the contamination assertion below would catch it.
  insert_player_skill(conn, "t20", "p1", "a1", batter_scoring_index = 1.4, batter_survival_rate = 0.98,
                      bowler_economy_index = 5.0, bowler_strike_rate = 0.99)
  # d2: a2 bats, p2 bowls. p2's OWN bowling values live here; a2's batting is
  # left NULL (untested) so this row cannot be mistaken for a2's batting skill.
  insert_player_skill(conn, "t20", "a2", "p2", bowler_economy_index = 0.8, bowler_strike_rate = 0.06,
                      delivery_id = "d2")

  # ONE row: India batting, Australia bowling -- a delivery-level row always
  # carries both sides at once. This resolves India's BATTING skill and
  # Australia's BOWLING skill together, while leaving India's bowling and
  # Australia's batting genuinely unseen (neither ever appears in the other
  # role in any row here), which is what the "role, not team" assertions below need.
  insert_team_skill(conn, "t20", "india_male_t20_international", "australia_male_t20_international",
                    batting_runs = 0.12, batting_wicket = -0.01, bowling_runs = -0.05, bowling_wicket = 0.02)

  insert_venue_skill(conn, "t20", "MCG", run_rate = 0.03, wicket_rate = -0.01,
                     boundary_rate = 0.18, dot_rate = 0.30)

  out <- build_match_simulation_inputs(conn, match_id = "m1")

  expect_equal(out$team1$source, "squad")
  expect_equal(out$team2$source, "squad")

  # p1's own batting skill, under the EXACT field names simulate_delivery() reads.
  # Located by value rather than assumed position, since bowlers get reordered.
  idx_p1 <- which(sapply(out$team1$batters, function(b) isTRUE(all.equal(b$batter_scoring_index, 1.4))))
  expect_length(idx_p1, 1)
  expect_setequal(names(out$team1$batters[[1]]),
                  c("batter_scoring_index", "batter_survival_rate", "batter_balls_faced"))
  expect_equal(out$team1$batters[[idx_p1]]$batter_scoring_index, 1.4)
  expect_equal(out$team1$batters[[idx_p1]]$batter_survival_rate, 0.98)

  # p2's own BOWLING skill.
  idx_p2_bowl <- which(sapply(out$team1$bowlers, function(b) isTRUE(all.equal(b$bowler_economy_index, 0.8))))
  expect_length(idx_p2_bowl, 1)
  expect_setequal(names(out$team1$bowlers[[1]]),
                  c("bowler_economy_index", "bowler_strike_rate", "bowler_balls_bowled"))
  expect_equal(out$team1$bowlers[[idx_p2_bowl]]$bowler_strike_rate, 0.06)

  # THE CONTAMINATION CHECK: p1 never appears as a bowler_id anywhere, so his
  # bowler_economy_index must be the league-average DEFAULT -- never a1's 5.0,
  # which sits in the same delivery_id row as p1's own batting values.
  idx_p1_bowl <- setdiff(seq_along(out$team1$bowlers), idx_p2_bowl)
  expect_length(idx_p1_bowl, 1)
  expect_equal(out$team1$bowlers[[idx_p1_bowl]]$bowler_economy_index,
               get_skill_start_values("t20")$economy_index)
  expect_false(isTRUE(all.equal(out$team1$bowlers[[idx_p1_bowl]]$bowler_economy_index, 5.0)))

  # team + venue skills under the exact names simulate_delivery()/simulate_innings() read.
  expect_equal(out$team1$team_skills$batting$runs_skill, 0.12)
  expect_true(out$team1$team_skills$batting$resolved)
  expect_equal(out$team1$team_skills$bowling$runs_skill, 0) # never seen bowling -> neutral, not batting's 0.12
  expect_false(out$team1$team_skills$bowling$resolved)
  expect_equal(out$team2$team_skills$bowling$runs_skill, -0.05)
  expect_true(out$team2$team_skills$bowling$resolved)
  expect_equal(out$team2$team_skills$batting$runs_skill, 0) # australia never seen batting here -> neutral
  expect_false(out$team2$team_skills$batting$resolved)

  expect_setequal(names(out$venue_skills),
                  c("venue_run_rate", "venue_wicket_rate", "venue_boundary_rate",
                    "venue_dot_rate", "venue_resolved"))
  expect_equal(out$venue_skills$venue_run_rate, 0.03)
  expect_true(out$venue_skills$venue_resolved)
})

test_that("a squad member with no skill row is counted, not silently defaulted", {
  conn <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
  make_fixture(conn, "t20")

  DBI::dbExecute(conn, "INSERT INTO cricsheet.matches VALUES
    ('m1', 'India', 'Australia', 'MCG', 'T20', 'male', 'international')")
  DBI::dbExecute(conn, "INSERT INTO main.match_squads VALUES
    ('m1', 'India', 'p1', 'Player One', TRUE),
    ('m1', 'India', 'ghost', 'Ghost Player', TRUE),
    ('m1', 'Australia', 'a1', 'Aussie One', TRUE)")
  insert_player_skill(conn, "t20", "p1", "a1", batter_scoring_index = 1.2, batter_survival_rate = 0.97)
  # "ghost" never appears in the skill table at all, and Australia's squad
  # (a1 only) has no skill row either -- an entire-team default, which must
  # fire the loud warning.

  expect_warning(
    out <- build_match_simulation_inputs(conn, match_id = "m1"),
    "LEAGUE-AVERAGE defaults"
  )

  expect_equal(out$team1$n_players, 2L)
  expect_equal(out$team1$n_batters_resolved, 1L)
  expect_equal(out$team1$n_bowlers_resolved, 0L)
  expect_equal(out$team1$unresolved_players, "Ghost Player")

  expect_equal(out$team2$n_players, 1L)
  expect_equal(out$team2$n_batters_resolved, 0L)
  expect_equal(out$team2$unresolved_players, "Aussie One")
})

test_that("a hypothetical fixture with no match_id uses caller-supplied names", {
  conn <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
  make_fixture(conn, "t20")

  DBI::dbExecute(conn, "INSERT INTO cricsheet.players VALUES
    ('p1', 'Player One'), ('a1', 'Aussie One')")
  insert_player_skill(conn, "t20", "p1", "a1", batter_scoring_index = 1.3, batter_survival_rate = 0.96,
                      bowler_economy_index = 1.0, bowler_strike_rate = 0.03)

  out <- build_match_simulation_inputs(
    conn, team1 = "India", team2 = "Australia", venue = "MCG", format = "t20",
    team1_players = "Player One", team2_players = "Aussie One"
  )

  expect_equal(out$team1$source, "caller_supplied")
  expect_equal(out$team2$source, "caller_supplied")
  expect_equal(out$team1$n_batters_resolved, 1L)
  expect_equal(out$team1$batters[[1]]$batter_scoring_index, 1.3)
  expect_length(out$team1$unresolved_players, 0)

  # No match_id and no players -> a clear error, not a silent empty roster.
  expect_error(
    build_match_simulation_inputs(conn, team1 = "India", team2 = "Australia",
                                  venue = "MCG", format = "t20"),
    "No squad found"
  )
})

test_that("simulate_match_ballbyball applies each team's OWN role skill, not the same object twice", {
  # Before bouncerverse#66's fix, team1_skills was reused unchanged as both
  # batting_team_skills (innings 1) and bowling_team_skills (innings 2) --
  # i.e. team1's batting-runs value would leak into its bowling-runs slot.
  team1_skills <- list(batting = list(runs_skill = 0.9, wicket_skill = 0),
                       bowling = list(runs_skill = -0.9, wicket_skill = 0))
  team2_skills <- list(batting = list(runs_skill = 0, wicket_skill = 0),
                       bowling = list(runs_skill = 0, wicket_skill = 0))

  captured <- list()
  fake_innings <- function(innings, batting_team_skills, bowling_team_skills, ...) {
    captured[[innings]] <<- list(batting = batting_team_skills, bowling = bowling_team_skills)
    list(total_runs = 100, wickets_lost = 3, balls_faced = 120,
         overs_faced = 20, overs_decimal = 20,
         ball_by_ball = data.table::data.table(), result = "completed")
  }
  testthat::local_mocked_bindings(simulate_innings = fake_innings, .package = "bouncer")

  simulate_match_ballbyball(
    model = NULL, format = "t20",
    team1_batters = list(), team1_bowlers = list(),
    team2_batters = list(), team2_bowlers = list(),
    team1_skills = team1_skills, team2_skills = team2_skills,
    venue_skills = list()
  )

  expect_equal(captured[[1]]$batting$runs_skill, 0.9)   # team1 batting: its own batting skill
  expect_equal(captured[[1]]$bowling$runs_skill, 0)     # team2 bowling: its own (neutral) bowling skill
  expect_equal(captured[[2]]$batting$runs_skill, 0)     # team2 batting: its own batting skill
  expect_equal(captured[[2]]$bowling$runs_skill, -0.9)  # team1 bowling: its own bowling skill, NOT 0.9
})

test_that("simulate_match_ballbyball still accepts a flat team skill list (backward compatible)", {
  captured <- list()
  fake_innings <- function(innings, batting_team_skills, bowling_team_skills, ...) {
    captured[[innings]] <<- list(batting = batting_team_skills, bowling = bowling_team_skills)
    list(total_runs = 50, wickets_lost = 2, balls_faced = 120,
         overs_faced = 20, overs_decimal = 20,
         ball_by_ball = data.table::data.table(), result = "completed")
  }
  testthat::local_mocked_bindings(simulate_innings = fake_innings, .package = "bouncer")

  flat1 <- list(runs_skill = 0.3, wicket_skill = 0)
  flat2 <- list(runs_skill = -0.1, wicket_skill = 0)
  simulate_match_ballbyball(
    model = NULL, format = "t20",
    team1_batters = list(), team1_bowlers = list(),
    team2_batters = list(), team2_bowlers = list(),
    team1_skills = flat1, team2_skills = flat2, venue_skills = list()
  )

  # A flat list has no $batting/$bowling, so %||% falls back to the whole
  # list for both roles -- exactly the pre-fix behaviour, preserved on purpose.
  expect_equal(captured[[1]]$batting$runs_skill, 0.3)
  expect_equal(captured[[2]]$bowling$runs_skill, 0.3)
})
