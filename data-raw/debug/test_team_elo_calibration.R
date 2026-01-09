# Debug script to test team ELO calibration
# This script runs the ACTUAL team ELO code (not duplicated logic)
# then adds calibration analysis
#
# Run from bouncer/ directory

library(cli)
library(dplyr)
devtools::load_all()

cli::cli_h1("Team ELO Calibration Test (Mens International Test)")

# ============================================================
# STEP 1: Run the actual team ELO calculation with filters
# ============================================================

# Set filters to only process Test International
FORMAT_FILTER <- "test"
CATEGORY_FILTER <- "mens_international"

# Source the main script's logic inline (with filters applied)
# This ensures we use the EXACT same code path

conn <- get_db_connection(read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

# Configuration from main script
CATEGORIES <- list(
  mens_international = list(gender = "male", team_type = "international")
)

FORMAT_GROUPS <- list(
  test = c("Test", "MDM")
)

params_dir <- file.path("..", "bouncerdata", "models")

# Load matches (same query as main script)
query <- "
  SELECT
    m.match_id,
    m.match_date,
    m.match_type,
    m.gender,
    m.team_type,
    m.season,
    m.event_name,
    m.venue,
    m.team1,
    m.team2,
    m.outcome_winner,
    m.outcome_type,
    m.outcome_by_runs,
    m.outcome_by_wickets,
    m.overs_per_innings,
    m.unified_margin,
    inn1.total_runs AS team1_score,
    inn1.total_overs AS team1_overs,
    inn2.total_runs AS team2_score,
    inn2.total_overs AS team2_overs
  FROM matches m
  LEFT JOIN match_innings inn1
    ON m.match_id = inn1.match_id AND inn1.innings = 1
  LEFT JOIN match_innings inn2
    ON m.match_id = inn2.match_id AND inn2.innings = 2
  WHERE m.outcome_winner IS NOT NULL
    AND m.outcome_winner != ''
    AND m.gender IS NOT NULL
    AND m.team_type IS NOT NULL
  ORDER BY m.match_date, m.match_id
"

all_matches <- DBI::dbGetQuery(conn, query)
cli::cli_alert_success("Loaded {nrow(all_matches)} total matches")

# Filter to Test International
current_format <- "test"
cat_config <- CATEGORIES[[CATEGORY_FILTER]]
format_match_types <- FORMAT_GROUPS[[current_format]]

matches <- all_matches %>%
  filter(
    gender == cat_config$gender,
    team_type == cat_config$team_type,
    match_type %in% format_match_types
  )

cli::cli_alert_success("Filtered to {nrow(matches)} Test International matches")

# Build home lookups (USING ACTUAL PACKAGE FUNCTION)
cli::cli_h2("Building home lookups")
format_home_lookups <- build_home_lookups(all_matches, current_format)
cli::cli_alert_success("Built venue lookup with {length(format_home_lookups$venue_country)} venues")

# Create team IDs and detect home team (SAME AS MAIN SCRIPT)
matches <- matches %>%
  mutate(
    format = current_format,
    team1_id = make_team_id_vec(team1, gender, format, team_type),
    team2_id = make_team_id_vec(team2, gender, format, team_type),
    outcome_winner_id = make_team_id_vec(outcome_winner, gender, format, team_type)
  ) %>%
  mutate(
    home_team = mapply(detect_home_team, team1, team2, team1_id, team2_id, venue, team_type,
                       MoreArgs = list(club_home_lookup = format_home_lookups$club_home,
                                       venue_country_lookup = format_home_lookups$venue_country))
  )

n_home <- sum(matches$home_team != 0)
cli::cli_alert_success("Home matches detected: {n_home}/{nrow(matches)} ({round(n_home/nrow(matches)*100, 1)}%)")

# Calculate unified margin (SAME AS MAIN SCRIPT)
matches <- matches %>%
  mutate(
    win_type = case_when(
      !is.na(outcome_by_runs) & outcome_by_runs > 0 ~ "runs",
      !is.na(outcome_by_wickets) & outcome_by_wickets > 0 ~ "wickets",
      outcome_type %in% c("tie", "draw", "no result") ~ outcome_type,
      TRUE ~ "unknown"
    ),
    overs_remaining = case_when(
      win_type == "wickets" & !is.na(overs_per_innings) & !is.na(team2_overs) ~
        pmax(0, overs_per_innings - team2_overs),
      TRUE ~ 0
    ),
    calc_unified_margin = mapply(
      function(t1_score, t2_score, overs_rem, wickets_rem, win_t, fmt, stored_margin) {
        if (!is.na(stored_margin)) return(stored_margin)
        if (is.na(t1_score) || is.na(t2_score)) return(NA_real_)
        if (win_t == "unknown") return(NA_real_)
        calculate_unified_margin(
          team1_score = t1_score,
          team2_score = t2_score,
          overs_remaining = overs_rem,
          wickets_remaining = wickets_rem,
          win_type = win_t,
          format = fmt
        )
      },
      team1_score, team2_score, overs_remaining,
      ifelse(is.na(outcome_by_wickets), 0L, outcome_by_wickets),
      win_type, format, unified_margin
    )
  )

# Load parameters (SAME AS MAIN SCRIPT)
cat_params <- load_category_params(CATEGORY_FILTER, params_dir, current_format)

K_MAX <- cat_params$K_MAX
K_MIN <- cat_params$K_MIN
K_HALFLIFE <- cat_params$K_HALFLIFE
HOME_ADVANTAGE <- cat_params$HOME_ADVANTAGE
STARTING_ELO <- 1300

cli::cli_alert_info("Parameters: K={K_MAX}->{K_MIN}, halflife={K_HALFLIFE}, home_adv={HOME_ADVANTAGE}")

# ============================================================
# STEP 2: Process matches and collect predictions for calibration
# ============================================================

cli::cli_h2("Processing matches and collecting predictions")

n_matches <- nrow(matches)
team_elos <- c()
team_matches_played <- c()
teams_seen <- character()

# Store predictions for calibration
predictions <- data.frame(
  match_id = character(n_matches),
  team1_id = character(n_matches),
  team2_id = character(n_matches),
  team1_elo = numeric(n_matches),
  team2_elo = numeric(n_matches),
  home_team = integer(n_matches),
  elo_diff = numeric(n_matches),
  pred_team1_win = numeric(n_matches),
  actual_team1_win = integer(n_matches),
  pred_margin = numeric(n_matches),
  actual_margin = numeric(n_matches),
  stringsAsFactors = FALSE
)

elo_divisor <- 400

cli::cli_progress_bar("Processing matches", total = n_matches)

for (i in seq_len(n_matches)) {
  team1_id <- matches$team1_id[i]
  team2_id <- matches$team2_id[i]
  team1_name <- matches$team1[i]
  team2_name <- matches$team2[i]
  winner <- matches$outcome_winner[i]
  home <- matches$home_team[i]
  margin <- matches$calc_unified_margin[i]

  # Add new teams at STARTING_ELO
 if (!(team1_id %in% teams_seen)) {
    teams_seen <- c(teams_seen, team1_id)
    team_elos[team1_id] <- STARTING_ELO
    team_matches_played[team1_id] <- 0L
  }
  if (!(team2_id %in% teams_seen)) {
    teams_seen <- c(teams_seen, team2_id)
    team_elos[team2_id] <- STARTING_ELO
    team_matches_played[team2_id] <- 0L
  }

  team1_elo <- team_elos[team1_id]
  team2_elo <- team_elos[team2_id]

  # Calculate prediction WITH home advantage (same as main script)
  home_adj <- home * HOME_ADVANTAGE
  elo_diff <- team1_elo - team2_elo + home_adj
  pred_team1_win <- 1 / (1 + 10^(-elo_diff / elo_divisor))

  # Predicted margin (runs per 100 ELO for Tests)
  runs_per_100_elo <- 7.5
  pred_margin <- elo_diff * runs_per_100_elo / 100

  # Actual outcomes
  team1_won <- !is.na(winner) && winner == team1_name
  team2_won <- !is.na(winner) && winner == team2_name
  actual_team1_win <- if (team1_won) 1L else if (team2_won) 0L else NA_integer_

  # Actual margin is ALREADY SIGNED in database (positive = team1 won)
  actual_margin <- margin

  # Store prediction
  predictions$match_id[i] <- matches$match_id[i]
  predictions$team1_id[i] <- team1_id
  predictions$team2_id[i] <- team2_id
  predictions$team1_elo[i] <- team1_elo
  predictions$team2_elo[i] <- team2_elo
  predictions$home_team[i] <- home
  predictions$elo_diff[i] <- elo_diff
  predictions$pred_team1_win[i] <- pred_team1_win
  predictions$actual_team1_win[i] <- actual_team1_win
  predictions$pred_margin[i] <- pred_margin
  predictions$actual_margin[i] <- actual_margin

  # Update ELOs (same logic as main script)
  if (team1_won || team2_won) {
    k1 <- K_MIN + (K_MAX - K_MIN) * exp(-team_matches_played[team1_id] / K_HALFLIFE)
    k2 <- K_MIN + (K_MAX - K_MIN) * exp(-team_matches_played[team2_id] / K_HALFLIFE)

    exp1 <- pred_team1_win
    exp2 <- 1 - exp1
    actual1 <- if (team1_won) 1 else 0
    actual2 <- if (team2_won) 1 else 0

    team_elos[team1_id] <- team1_elo + k1 * (actual1 - exp1)
    team_elos[team2_id] <- team2_elo + k2 * (actual2 - exp2)
  }

  team_matches_played[team1_id] <- team_matches_played[team1_id] + 1L
  team_matches_played[team2_id] <- team_matches_played[team2_id] + 1L

  # Normalize to maintain mean of 1500
  drift <- mean(team_elos) - 1500
  if (abs(drift) > 0.001) {
    team_elos <- team_elos - drift
  }

  if (i %% 100 == 0) cli::cli_progress_update(set = i)
}

cli::cli_progress_done()

# ============================================================
# STEP 3: Final standings
# ============================================================

cli::cli_h2("Final Standings")

final_elos <- data.frame(
  team_id = names(team_elos),
  elo = unname(team_elos),
  matches = unname(team_matches_played)
) %>%
  mutate(team_name = gsub("_male_test_international$", "", team_id)) %>%
  arrange(desc(elo))

cli::cli_h3("Top 10 Teams")
for (j in 1:min(10, nrow(final_elos))) {
  cli::cli_alert_success("{j}. {final_elos$team_name[j]}: {round(final_elos$elo[j])} ({final_elos$matches[j]} matches)")
}

cli::cli_h3("Bottom 10 Teams")
bottom <- tail(final_elos, 10)
for (j in 1:nrow(bottom)) {
  rank <- nrow(final_elos) - 10 + j
  cli::cli_alert_warning("{rank}. {bottom$team_name[j]}: {round(bottom$elo[j])} ({bottom$matches[j]} matches)")
}

# ============================================================
# STEP 4: Calibration Analysis
# ============================================================

cli::cli_h1("Calibration Analysis")

valid <- predictions[!is.na(predictions$actual_team1_win), ]
cli::cli_alert_info("Matches with outcomes: {nrow(valid)}")

# ---- WIN PROBABILITY CALIBRATION ----
cli::cli_h2("Win Probability Calibration")

valid$pred_bin <- cut(
  valid$pred_team1_win,
  breaks = c(0, 0.2, 0.35, 0.45, 0.55, 0.65, 0.8, 1.0),
  labels = c("0-20%", "20-35%", "35-45%", "45-55%", "55-65%", "65-80%", "80-100%"),
  include.lowest = TRUE
)

win_cal <- valid %>%
  group_by(pred_bin) %>%
  summarise(
    n = n(),
    pred = mean(pred_team1_win),
    actual = mean(actual_team1_win),
    error = actual - pred,
    .groups = "drop"
  )

cat(sprintf("%-12s %8s %10s %10s %10s\n", "Pred Bin", "N", "Pred%", "Actual%", "Error"))
cat(paste(rep("-", 55), collapse = ""), "\n")
for (j in seq_len(nrow(win_cal))) {
  cat(sprintf("%-12s %8d %9.1f%% %9.1f%% %+9.1f%%\n",
              as.character(win_cal$pred_bin[j]), win_cal$n[j],
              win_cal$pred[j]*100, win_cal$actual[j]*100, win_cal$error[j]*100))
}

brier <- mean((valid$pred_team1_win - valid$actual_team1_win)^2)
accuracy <- mean((valid$pred_team1_win > 0.5) == valid$actual_team1_win)
cli::cli_alert_info("Brier Score: {round(brier, 4)} (0.25 = random)")
cli::cli_alert_info("Accuracy: {round(accuracy*100, 1)}%")

# ---- MARGIN CALIBRATION ----
cli::cli_h2("Margin of Victory Calibration")

margin_valid <- predictions[!is.na(predictions$actual_margin), ]
cli::cli_alert_info("Matches with margin: {nrow(margin_valid)}")

margin_valid$pred_bin <- cut(
  margin_valid$pred_margin,
  breaks = c(-Inf, -50, -20, -5, 5, 20, 50, Inf),
  labels = c("<-50", "-50:-20", "-20:-5", "-5:+5", "+5:+20", "+20:+50", ">+50")
)

margin_cal <- margin_valid %>%
  group_by(pred_bin) %>%
  summarise(
    n = n(),
    pred_mean = mean(pred_margin),
    actual_mean = mean(actual_margin),
    error = actual_mean - pred_mean,
    mae = mean(abs(actual_margin - pred_margin)),
    .groups = "drop"
  )

cat(sprintf("%-12s %6s %10s %10s %8s %8s\n", "Pred Bin", "N", "Pred", "Actual", "Error", "MAE"))
cat(paste(rep("-", 60), collapse = ""), "\n")
for (j in seq_len(nrow(margin_cal))) {
  cat(sprintf("%-12s %6d %+9.1f %+9.1f %+7.1f %7.1f\n",
              as.character(margin_cal$pred_bin[j]), margin_cal$n[j],
              margin_cal$pred_mean[j], margin_cal$actual_mean[j],
              margin_cal$error[j], margin_cal$mae[j]))
}

margin_cor <- cor(margin_valid$pred_margin, margin_valid$actual_margin, use = "complete.obs")
overall_mae <- mean(abs(margin_valid$actual_margin - margin_valid$pred_margin), na.rm = TRUE)
cli::cli_alert_info("Margin Correlation: {round(margin_cor, 3)}")
cli::cli_alert_info("Overall MAE: {round(overall_mae, 1)} runs")

# ---- HOME/AWAY CALIBRATION ----
cli::cli_h2("Home/Away Calibration")

home_m <- valid[valid$home_team == 1, ]
away_m <- valid[valid$home_team == -1, ]
neutral_m <- valid[valid$home_team == 0, ]

cli::cli_alert_info("Home (team1): {nrow(home_m)}, Away (team2 home): {nrow(away_m)}, Neutral: {nrow(neutral_m)}")

if (nrow(home_m) > 20) {
  cli::cli_alert_info("Team1 HOME: Pred {round(mean(home_m$pred_team1_win)*100,1)}%, Actual {round(mean(home_m$actual_team1_win)*100,1)}%")
}
if (nrow(away_m) > 20) {
  cli::cli_alert_info("Team1 AWAY: Pred {round(mean(away_m$pred_team1_win)*100,1)}%, Actual {round(mean(away_m$actual_team1_win)*100,1)}%")
}
if (nrow(neutral_m) > 20) {
  cli::cli_alert_info("NEUTRAL: Pred {round(mean(neutral_m$pred_team1_win)*100,1)}%, Actual {round(mean(neutral_m$actual_team1_win)*100,1)}%")
}

cli::cli_alert_success("Calibration complete!")
