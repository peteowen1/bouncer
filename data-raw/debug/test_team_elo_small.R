# Debug script to test new team ELO schema - small test
# Run from bouncer/ directory

library(cli)
library(dplyr)
devtools::load_all()

cli::cli_h1("Testing Team ELO with New Schema (Test International Only)")

# Configuration - just test format, mens international
FORMATS <- c("test")
CATEGORIES <- list(
  mens_international = list(gender = "male", team_type = "international")
)

FORMAT_GROUPS <- list(
  test = c("Test", "MDM")
)

params_dir <- file.path("..", "bouncerdata", "models")
output_dir <- file.path("..", "bouncerdata", "models")

# Connect to database
conn <- get_db_connection(read_only = FALSE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

# Get matches
all_category_results <- list()

for (current_format in FORMATS) {
  format_types <- FORMAT_GROUPS[[current_format]]

  for (category_name in names(CATEGORIES)) {
    cat_config <- CATEGORIES[[category_name]]

    cli::cli_h2("Category: {category_name} / Format: {current_format}")

    # Load matches for this category/format
    format_clause <- paste(sprintf("'%s'", tolower(format_types)), collapse = ", ")
    query <- sprintf("
      SELECT
        m.match_id,
        m.match_date,
        m.match_type,
        m.event_name,
        m.team1,
        m.team2,
        m.outcome_winner AS outcome_winner_id,
        m.gender,
        m.team_type,
        m.venue,
        m.unified_margin,
        0 as home_team,
        COALESCE(EXTRACT(YEAR FROM m.match_date), 2020) as season
      FROM matches m
      WHERE LOWER(m.match_type) IN (%s)
        AND m.gender = '%s'
        AND m.team_type = '%s'
        AND m.outcome_type IS NOT NULL
      ORDER BY m.match_date, m.match_id
    ", format_clause, cat_config$gender, cat_config$team_type)

    matches <- DBI::dbGetQuery(conn, query)

    if (nrow(matches) == 0) {
      cli::cli_alert_warning("No matches found - skipping")
      next
    }

    cli::cli_alert_success("Found {nrow(matches)} matches")

    # Create team IDs
    matches$team1_id <- paste0(
      tolower(gsub(" ", "_", matches$team1)), "_",
      matches$gender, "_", current_format, "_", matches$team_type
    )
    matches$team2_id <- paste0(
      tolower(gsub(" ", "_", matches$team2)), "_",
      matches$gender, "_", current_format, "_", matches$team_type
    )

    # Prepare margin
    matches$calc_unified_margin <- matches$unified_margin

    all_teams_eventual <- unique(c(matches$team1_id, matches$team2_id))
    cli::cli_alert_info("Will see {length(all_teams_eventual)} unique teams total")

    # Initialize ELOs - teams added dynamically as they first appear
    # New teams start at 1300 (below average) and must earn their way up
    STARTING_ELO <- 1300
    team_result_elos <- c()  # Empty - teams added as they appear
    team_matches_played <- c()
    teams_seen <- character()

    cli::cli_alert_info("New teams start at {STARTING_ELO} ELO (below 1500 average)")

    # Simple ELO parameters
    K_MAX <- 58
    K_MIN <- 16
    K_HALFLIFE <- 25
    HOME_ADVANTAGE <- 50

    # Process matches
    n_matches <- nrow(matches)
    n_teams <- length(all_teams_eventual)
    n_records <- n_matches * n_teams

    cli::cli_alert_info("Will generate {format(n_records, big.mark=',')} records ({n_matches} matches x {n_teams} teams)")

    # Pre-allocate output
    out_team_id <- character(n_records)
    out_match_id <- character(n_records)
    out_match_date <- as.Date(rep(NA, n_records))
    out_format <- character(n_records)
    out_gender <- character(n_records)
    out_team_type <- character(n_records)
    out_elo_before <- numeric(n_records)
    out_elo_after <- numeric(n_records)
    out_elo_diff <- numeric(n_records)
    out_played_in_match <- logical(n_records)
    out_matches_played <- integer(n_records)

    record_idx <- 1L
    elo_divisor <- 400

    cli::cli_progress_bar("Processing matches", total = n_matches)

    for (i in seq_len(n_matches)) {
      team1_id <- matches$team1_id[i]
      team2_id <- matches$team2_id[i]
      winner_id <- matches$outcome_winner_id[i]
      match_id <- matches$match_id[i]
      match_date <- matches$match_date[i]

      # Add new teams dynamically at STARTING_ELO (below average)
      if (!(team1_id %in% teams_seen)) {
        teams_seen <- c(teams_seen, team1_id)
        team_result_elos[team1_id] <- STARTING_ELO
        team_matches_played[team1_id] <- 0L
      }
      if (!(team2_id %in% teams_seen)) {
        teams_seen <- c(teams_seen, team2_id)
        team_result_elos[team2_id] <- STARTING_ELO
        team_matches_played[team2_id] <- 0L
      }

      team1_elo_before <- team_result_elos[team1_id]
      team2_elo_before <- team_result_elos[team2_id]

      team1_won <- !is.na(winner_id) && winner_id == matches$team1[i]
      team2_won <- !is.na(winner_id) && winner_id == matches$team2[i]

      if (team1_won || team2_won) {
        # Dynamic K based on matches played
        k1 <- K_MIN + (K_MAX - K_MIN) * exp(-team_matches_played[team1_id] / K_HALFLIFE)
        k2 <- K_MIN + (K_MAX - K_MIN) * exp(-team_matches_played[team2_id] / K_HALFLIFE)

        # Home advantage
        home_adj <- matches$home_team[i] * HOME_ADVANTAGE

        # Expected scores
        exp1 <- 1 / (1 + 10^((team2_elo_before - team1_elo_before - home_adj) / elo_divisor))
        exp2 <- 1 - exp1

        actual1 <- if (team1_won) 1 else 0
        actual2 <- if (team2_won) 1 else 0

        team1_elo_after <- team1_elo_before + k1 * (actual1 - exp1)
        team2_elo_after <- team2_elo_before + k2 * (actual2 - exp2)
      } else {
        team1_elo_after <- team1_elo_before
        team2_elo_after <- team2_elo_before
      }

      # Update state
      team_result_elos[team1_id] <- team1_elo_after
      team_result_elos[team2_id] <- team2_elo_after
      team_matches_played[team1_id] <- team_matches_played[team1_id] + 1L
      team_matches_played[team2_id] <- team_matches_played[team2_id] + 1L

      # Capture elo_before for ALL teams (before normalization)
      all_elo_before <- team_result_elos
      all_elo_before[team1_id] <- team1_elo_before
      all_elo_before[team2_id] <- team2_elo_before

      # Normalize ALL teams to maintain mean of 1500
      current_mean <- mean(team_result_elos)
      drift <- current_mean - 1500
      if (abs(drift) > 0.001) {
        team_result_elos <- team_result_elos - drift
      }

      # Store a row for EVERY team seen so far
      for (team_id in teams_seen) {
        out_team_id[record_idx] <- team_id
        out_match_id[record_idx] <- match_id
        out_match_date[record_idx] <- match_date
        out_format[record_idx] <- current_format
        out_gender[record_idx] <- cat_config$gender
        out_team_type[record_idx] <- cat_config$team_type
        out_elo_before[record_idx] <- all_elo_before[team_id]
        out_elo_after[record_idx] <- team_result_elos[team_id]
        out_elo_diff[record_idx] <- team_result_elos[team_id] - all_elo_before[team_id]
        out_played_in_match[record_idx] <- (team_id == team1_id || team_id == team2_id)
        out_matches_played[record_idx] <- team_matches_played[team_id]
        record_idx <- record_idx + 1L
      }

      if (i %% 100 == 0) cli::cli_progress_update(set = i)
    }

    cli::cli_progress_done()

    # Create data frame
    actual_records <- record_idx - 1L
    elo_df <- data.frame(
      team_id = out_team_id[1:actual_records],
      match_id = out_match_id[1:actual_records],
      match_date = out_match_date[1:actual_records],
      format = out_format[1:actual_records],
      gender = out_gender[1:actual_records],
      team_type = out_team_type[1:actual_records],
      elo_before = out_elo_before[1:actual_records],
      elo_after = out_elo_after[1:actual_records],
      elo_diff = out_elo_diff[1:actual_records],
      played_in_match = out_played_in_match[1:actual_records],
      matches_played = out_matches_played[1:actual_records],
      stringsAsFactors = FALSE
    )

    cli::cli_alert_success("Generated {nrow(elo_df)} records")

    # Show sample of results
    cli::cli_h3("Sample: Last match")
    last_match_id <- tail(unique(elo_df$match_id), 1)
    last_match <- elo_df[elo_df$match_id == last_match_id, ]
    last_match <- last_match[order(-abs(last_match$elo_diff)), ]
    print(head(last_match, 10))

    # Show final standings
    cli::cli_h3("Final Top 10 Teams")
    final_elos <- data.frame(
      team_id = names(team_result_elos),
      elo = unname(team_result_elos),
      matches = unname(team_matches_played)
    ) %>% arrange(desc(elo))

    print(head(final_elos, 10))

    # Verify mean is 1500
    cli::cli_alert_info("Final mean ELO: {round(mean(team_result_elos), 4)}")

    # Show ALL teams sorted by ELO
    cli::cli_h3("All Teams (sorted by ELO)")
    print(final_elos)

    # Show distribution of non-playing team adjustments
    non_playing <- elo_df[!elo_df$played_in_match, ]
    cli::cli_alert_info("Non-playing team adjustments: mean={round(mean(non_playing$elo_diff), 6)}, sd={round(sd(non_playing$elo_diff), 4)}")
  }
}

cli::cli_alert_success("Test complete!")
