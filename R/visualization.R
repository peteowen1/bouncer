# Visualization Functions for Bouncer
#
# ggplot2-based plotting functions for cricket analytics visualization.
# All functions return ggplot objects that can be further customized.


#' Bouncer ggplot2 Theme
#'
#' A clean, consistent theme for bouncer visualizations.
#'
#' @param base_size Base font size.
#' @param base_family Base font family.
#'
#' @return A ggplot2 theme object.
#' @export
#'
#' @examples
#' \dontrun{
#' library(ggplot2)
#' ggplot(data, aes(x, y)) + geom_point() + theme_bouncer()
#' }
theme_bouncer <- function(base_size = 12, base_family = "") {

  if (!requireNamespace("ggplot2", quietly = TRUE)) {
    cli::cli_abort("Package 'ggplot2' is required for visualization functions")
  }

  ggplot2::theme_minimal(base_size = base_size, base_family = base_family) +
    ggplot2::theme(
      # Panel
      panel.grid.minor = ggplot2::element_blank(),
      panel.grid.major = ggplot2::element_line(color = "#E0E0E0", linewidth = 0.3),
      panel.background = ggplot2::element_rect(fill = "white", color = NA),

      # Axis
      axis.title = ggplot2::element_text(size = base_size * 0.9, color = "#333333"),
      axis.text = ggplot2::element_text(size = base_size * 0.8, color = "#666666"),
      axis.line = ggplot2::element_line(color = "#333333", linewidth = 0.5),

      # Legend
      legend.position = "bottom",
      legend.title = ggplot2::element_text(size = base_size * 0.85),
      legend.text = ggplot2::element_text(size = base_size * 0.8),

      # Plot title
      plot.title = ggplot2::element_text(
        size = base_size * 1.2,
        face = "bold",
        color = "#333333",
        hjust = 0
      ),
      plot.subtitle = ggplot2::element_text(
        size = base_size * 0.9,
        color = "#666666",
        hjust = 0
      ),
      plot.caption = ggplot2::element_text(
        size = base_size * 0.7,
        color = "#999999",
        hjust = 1
      ),

      # Margins
      plot.margin = ggplot2::margin(10, 10, 10, 10)
    )
}


# ============================================================================
# Match Visualization
# ============================================================================

#' Plot Score Progression
#'
#' Creates a line chart showing cumulative runs over the course of a match.
#'
#' @param match_id Character. Match ID to visualize.
#' @param db_path Character. Database path.
#' @param show_wickets Logical. If TRUE, marks wickets on the chart.
#'
#' @return A ggplot object.
#' @export
#'
#' @examples
#' \dontrun{
#' plot_score_progression("1234567")
#' }
plot_score_progression <- function(match_id, db_path = NULL, show_wickets = TRUE) {

  if (!requireNamespace("ggplot2", quietly = TRUE)) {
    cli::cli_abort("Package 'ggplot2' is required")
  }

  # Get match deliveries
  deliveries <- query_deliveries(match_id = match_id, db_path = db_path)

  if (nrow(deliveries) == 0) {
    cli::cli_alert_warning("No deliveries found for match {match_id}")
    return(NULL)
  }

  # Calculate cumulative runs and ball number per innings
  deliveries <- deliveries %>%
    dplyr::group_by(innings) %>%
    dplyr::arrange(over, ball) %>%
    dplyr::mutate(
      ball_number = dplyr::row_number(),
      cumulative_runs = cumsum(runs_total)
    ) %>%
    dplyr::ungroup()

  # Get team names for labels
  teams <- unique(deliveries$batting_team)
  team_colors <- c("#1E88E5", "#E53935")  # Blue and Red
  names(team_colors) <- teams[1:min(2, length(teams))]

  # Build plot
  p <- ggplot2::ggplot(deliveries, ggplot2::aes(
    x = ball_number,
    y = cumulative_runs,
    color = batting_team
  )) +
    ggplot2::geom_line(linewidth = 1) +
    ggplot2::scale_color_manual(values = team_colors, name = "Team") +
    ggplot2::labs(
      title = "Score Progression",
      subtitle = paste(teams, collapse = " vs "),
      x = "Ball Number",
      y = "Cumulative Runs",
      caption = paste("Match ID:", match_id)
    ) +
    theme_bouncer()

  # Add wicket markers
  if (show_wickets) {
    wicket_balls <- deliveries[deliveries$is_wicket == TRUE, ]
    if (nrow(wicket_balls) > 0) {
      p <- p +
        ggplot2::geom_point(
          data = wicket_balls,
          ggplot2::aes(x = ball_number, y = cumulative_runs),
          shape = 4, size = 3, stroke = 1.5
        )
    }
  }

  return(p)
}


#' Plot Win Probability Over Match
#'
#' Creates a chart showing how win probability changed throughout a match.
#'
#' @param match_id Character. Match ID to visualize.
#' @param format Character. Match format for model loading.
#' @param db_path Character. Database path.
#'
#' @return A ggplot object.
#' @export
#'
#' @examples
#' \dontrun{
#' plot_win_probability("1234567", format = "t20")
#' }
plot_win_probability <- function(match_id, format = "t20", db_path = NULL) {

  if (!requireNamespace("ggplot2", quietly = TRUE)) {
    cli::cli_abort("Package 'ggplot2' is required")
  }

  # Get deliveries with win probability
  deliveries <- query_deliveries(match_id = match_id, db_path = db_path)

  if (nrow(deliveries) == 0) {
    cli::cli_alert_warning("No deliveries found for match {match_id}")
    return(NULL)
  }

  # Add win probability (this may be slow for large matches)
  n_balls <- nrow(deliveries)
  cli::cli_alert_info("Calculating win probabilities for {n_balls} deliveries...")
  cli::cli_progress_bar("Processing", total = 1, clear = FALSE)
  deliveries <- tryCatch({
    result <- add_win_probability(deliveries, format = format)
    cli::cli_progress_done()
    result
  }, error = function(e) {
    cli::cli_progress_done()
    cli::cli_warn("Could not calculate win probability: {e$message}")
    return(deliveries)
  })

  # Check if we have win probability data
  if (!"win_prob_after" %in% names(deliveries)) {
    cli::cli_alert_warning("Win probability calculation not available")
    return(NULL)
  }

  # Add ball number across whole match
  deliveries <- deliveries %>%
    dplyr::arrange(innings, over, ball) %>%
    dplyr::mutate(match_ball = dplyr::row_number())

  # Get team names
  teams <- unique(deliveries$batting_team)
  team1 <- teams[1]

  # Build plot
  p <- ggplot2::ggplot(deliveries, ggplot2::aes(
    x = match_ball,
    y = win_prob_after * 100
  )) +
    ggplot2::geom_line(color = "#1E88E5", linewidth = 1) +
    ggplot2::geom_hline(yintercept = 50, linetype = "dashed", color = "#999999") +
    ggplot2::geom_ribbon(
      ggplot2::aes(ymin = 50, ymax = pmax(win_prob_after * 100, 50)),
      fill = "#1E88E5", alpha = 0.2
    ) +
    ggplot2::geom_ribbon(
      ggplot2::aes(ymin = pmin(win_prob_after * 100, 50), ymax = 50),
      fill = "#E53935", alpha = 0.2
    ) +
    ggplot2::scale_y_continuous(limits = c(0, 100), breaks = seq(0, 100, 25)) +
    ggplot2::labs(
      title = "Win Probability",
      subtitle = paste(teams, collapse = " vs "),
      x = "Ball Number",
      y = paste0(team1, " Win Probability (%)"),
      caption = paste("Match ID:", match_id)
    ) +
    theme_bouncer()

  # Add innings separator line
  innings1_balls <- sum(deliveries$innings == 1)
  if (innings1_balls > 0 && innings1_balls < nrow(deliveries)) {
    p <- p +
      ggplot2::geom_vline(
        xintercept = innings1_balls + 0.5,
        linetype = "solid",
        color = "#333333",
        linewidth = 0.5
      ) +
      ggplot2::annotate(
        "text", x = innings1_balls + 0.5, y = 95,
        label = "Innings Break", hjust = -0.1, size = 3
      )
  }

  return(p)
}


# ============================================================================
# Player Visualization
# ============================================================================

#' Plot Player Skill Progression
#'
#' Creates a line chart showing how a player's skill indices have changed over time.
#'
#' @param player_name Character. Player name to look up.
#' @param format Character. Match format: "t20", "odi", "test".
#' @param skill Character. Which skill to plot: "scoring", "survival", "economy",
#'   "strike", or "all".
#' @param n_recent Integer. Number of recent matches to show. If NULL, shows all.
#' @param db_path Character. Database path.
#'
#' @return A ggplot object.
#' @export
#'
#' @examples
#' \dontrun{
#' plot_skill_progression("Virat Kohli", format = "t20")
#' plot_skill_progression("Jasprit Bumrah", format = "t20", skill = "economy")
#' }
plot_skill_progression <- function(player_name,
                                    format = "t20",
                                    skill = "all",
                                    n_recent = NULL,
                                    db_path = NULL) {

  if (!requireNamespace("ggplot2", quietly = TRUE)) {
    cli::cli_abort("Package 'ggplot2' is required")
  }

  # Get player info
  player <- get_player(player_name, format = format, db_path = db_path)

  if (is.null(player)) {
    return(NULL)
  }

  conn <- get_db_connection(path = db_path, read_only = TRUE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

  # Get skill history
  format_lower <- tolower(format)
  skill_table <- paste0(format_lower, "_player_skill")

  # Check if table exists
  if (!skill_table %in% DBI::dbListTables(conn)) {
    cli::cli_alert_warning("Skill table not found: {skill_table}")
    return(NULL)
  }

  # Query skill history
  query <- sprintf("
    SELECT
      s.delivery_id,
      d.match_date,
      d.match_id,
      s.batter_scoring_index,
      s.batter_survival_rate,
      s.bowler_economy_index,
      s.bowler_strike_rate
    FROM %s s
    JOIN cricsheet.deliveries d ON s.delivery_id = d.delivery_id
    WHERE s.batter_id = ? OR s.bowler_id = ?
    ORDER BY d.match_date ASC
  ", skill_table)

  skill_data <- DBI::dbGetQuery(conn, query, params = list(player$player_id, player$player_id))

  if (nrow(skill_data) == 0) {
    cli::cli_alert_warning("No skill data found for {player_name}")
    return(NULL)
  }

  # Sample to one point per match for cleaner visualization
  skill_data <- skill_data %>%
    dplyr::group_by(match_id) %>%
    dplyr::slice_tail(n = 1) %>%
    dplyr::ungroup()

  # Limit to recent matches if specified
  if (!is.null(n_recent)) {
    skill_data <- dplyr::slice_tail(skill_data, n = n_recent)
  }

  # Add row number for x-axis
  skill_data$match_num <- seq_len(nrow(skill_data))

  # Determine which skills to plot
  skill <- tolower(skill)

  if (skill == "all") {
    # Reshape for faceting
    plot_data <- skill_data %>%
      tidyr::pivot_longer(
        cols = c(batter_scoring_index, batter_survival_rate,
                 bowler_economy_index, bowler_strike_rate),
        names_to = "skill_type",
        values_to = "value"
      ) %>%
      dplyr::filter(!is.na(value))

    # Clean up skill names
    plot_data$skill_type <- dplyr::case_when(
      plot_data$skill_type == "batter_scoring_index" ~ "Batting Scoring",
      plot_data$skill_type == "batter_survival_rate" ~ "Batting Survival",
      plot_data$skill_type == "bowler_economy_index" ~ "Bowling Economy",
      plot_data$skill_type == "bowler_strike_rate" ~ "Bowling Strike",
      TRUE ~ plot_data$skill_type
    )

    p <- ggplot2::ggplot(plot_data, ggplot2::aes(
      x = match_num,
      y = value,
      color = skill_type
    )) +
      ggplot2::geom_line(linewidth = 1) +
      ggplot2::facet_wrap(~skill_type, scales = "free_y", ncol = 2) +
      ggplot2::labs(
        title = paste(player$player_name, "- Skill Progression"),
        subtitle = toupper(format),
        x = "Match Number",
        y = "Skill Index Value"
      ) +
      theme_bouncer() +
      ggplot2::theme(legend.position = "none")

  } else {
    # Plot single skill
    skill_col <- switch(skill,
      "scoring" = "batter_scoring_index",
      "survival" = "batter_survival_rate",
      "economy" = "bowler_economy_index",
      "strike" = "bowler_strike_rate",
      "batter_scoring_index"
    )

    skill_label <- switch(skill,
      "scoring" = "Batting Scoring Index",
      "survival" = "Batting Survival Rate",
      "economy" = "Bowling Economy Index",
      "strike" = "Bowling Strike Rate",
      "Skill Index"
    )

    p <- ggplot2::ggplot(skill_data, ggplot2::aes(
      x = match_num,
      y = .data[[skill_col]]
    )) +
      ggplot2::geom_line(color = "#1E88E5", linewidth = 1) +
      ggplot2::geom_smooth(method = "loess", se = FALSE, color = "#999999",
                           linetype = "dashed", linewidth = 0.5) +
      ggplot2::labs(
        title = paste(player$player_name, "-", skill_label),
        subtitle = toupper(format),
        x = "Match Number",
        y = skill_label
      ) +
      theme_bouncer()
  }

  return(p)
}


#' Plot Player Comparison
#'
#' Creates a bar chart comparing two players' key metrics.
#'
#' @param player1 Character. First player name.
#' @param player2 Character. Second player name.
#' @param format Character. Match format.
#' @param db_path Character. Database path.
#'
#' @return A ggplot object.
#' @export
#'
#' @examples
#' \dontrun{
#' plot_player_comparison("Virat Kohli", "Steve Smith", format = "test")
#' }
plot_player_comparison <- function(player1, player2, format = "t20", db_path = NULL) {

  if (!requireNamespace("ggplot2", quietly = TRUE)) {
    cli::cli_abort("Package 'ggplot2' is required")
  }

  # Get comparison data
  comparison <- compare_players(player1, player2, format = format, db_path = db_path)

  if (is.null(comparison)) {
    return(NULL)
  }

  # Build comparison data frame for batting
  plot_data <- data.frame(
    Player = rep(c(comparison$player1$player$player_name,
                   comparison$player2$player$player_name), each = 3),
    Metric = rep(c("Runs", "Average", "Strike Rate"), 2),
    Value = c(
      comparison$player1$batting$runs_scored,
      comparison$player1$batting$batting_average %||% 0,
      comparison$player1$batting$strike_rate,
      comparison$player2$batting$runs_scored,
      comparison$player2$batting$batting_average %||% 0,
      comparison$player2$batting$strike_rate
    ),
    stringsAsFactors = FALSE
  )

  # Handle NA values
  plot_data$Value[is.na(plot_data$Value)] <- 0

  # Normalize values for comparison (percentage of max for each metric)
  plot_data <- plot_data %>%
    dplyr::group_by(Metric) %>%
    dplyr::mutate(
      NormValue = Value / max(Value, na.rm = TRUE) * 100
    ) %>%
    dplyr::ungroup()

  # Build plot
  p <- ggplot2::ggplot(plot_data, ggplot2::aes(
    x = Metric,
    y = NormValue,
    fill = Player
  )) +
    ggplot2::geom_col(position = ggplot2::position_dodge(width = 0.8), width = 0.7) +
    ggplot2::geom_text(
      ggplot2::aes(label = round(Value, 1)),
      position = ggplot2::position_dodge(width = 0.8),
      vjust = -0.3,
      size = 3
    ) +
    ggplot2::scale_fill_manual(values = c("#1E88E5", "#E53935")) +
    ggplot2::labs(
      title = "Player Comparison (Batting)",
      subtitle = toupper(format),
      x = NULL,
      y = "Relative Performance (%)",
      fill = "Player"
    ) +
    theme_bouncer()

  return(p)
}


# ============================================================================
# Team Visualization
# ============================================================================

#' Plot Team ELO History
#'
#' Creates a line chart showing a team's ELO rating over time.
#'
#' @param team_name Character. Team name.
#' @param format Character. Match format.
#' @param n_matches Integer. Number of recent matches to show. If NULL, shows all.
#' @param db_path Character. Database path.
#'
#' @return A ggplot object.
#' @export
#'
#' @examples
#' \dontrun{
#' plot_elo_history("India", format = "t20")
#' }
plot_elo_history <- function(team_name,
                              format = NULL,
                              n_matches = NULL,
                              db_path = NULL) {

  if (!requireNamespace("ggplot2", quietly = TRUE)) {
    cli::cli_abort("Package 'ggplot2' is required")
  }

  conn <- get_db_connection(path = db_path, read_only = TRUE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

  # Build query
  format_filter <- ""
  if (!is.null(format)) {
    format <- tolower(format)
    format_filter <- switch(format,
      "t20" = "AND LOWER(match_type) IN ('t20', 'it20')",
      "odi" = "AND LOWER(match_type) IN ('odi', 'odm')",
      "test" = "AND LOWER(match_type) IN ('test', 'mdm')",
      ""
    )
  }

  query <- sprintf("
    SELECT
      match_date,
      match_id,
      match_type,
      elo_result,
      elo_roster_combined
    FROM team_elo
    WHERE LOWER(team_id) LIKE LOWER(?)
    %s
    ORDER BY match_date ASC
  ", format_filter)

  elo_data <- DBI::dbGetQuery(conn, query, params = list(paste0("%", team_name, "%")))

  if (nrow(elo_data) == 0) {
    cli::cli_alert_warning("No ELO data found for {team_name}")
    return(NULL)
  }

  # Limit to recent matches if specified
  if (!is.null(n_matches)) {
    elo_data <- dplyr::slice_tail(elo_data, n = n_matches)
  }

  # Add match number
  elo_data$match_num <- seq_len(nrow(elo_data))

  # Build plot
  p <- ggplot2::ggplot(elo_data, ggplot2::aes(x = match_date, y = elo_result)) +
    ggplot2::geom_line(color = "#1E88E5", linewidth = 1) +
    ggplot2::geom_hline(yintercept = 1500, linetype = "dashed", color = "#999999") +
    ggplot2::geom_smooth(method = "loess", se = FALSE, color = "#E53935",
                         linetype = "dashed", linewidth = 0.5) +
    ggplot2::labs(
      title = paste(team_name, "- ELO Rating History"),
      subtitle = if (!is.null(format)) toupper(format) else "All Formats",
      x = "Date",
      y = "ELO Rating",
      caption = "Dashed line = 1500 (average), Red = trend"
    ) +
    theme_bouncer()

  return(p)
}


#' Plot Team Strength Breakdown
#'
#' Creates a bar chart showing a team's current skill components.
#'
#' @param team_name Character. Team name.
#' @param format Character. Match format.
#' @param db_path Character. Database path.
#'
#' @return A ggplot object.
#' @export
#'
#' @examples
#' \dontrun{
#' plot_team_strength("India", format = "t20")
#' }
plot_team_strength <- function(team_name, format = "t20", db_path = NULL) {

  if (!requireNamespace("ggplot2", quietly = TRUE)) {
    cli::cli_abort("Package 'ggplot2' is required")
  }

  conn <- get_db_connection(path = db_path, read_only = TRUE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

  # Get team skill data
  format_lower <- tolower(format)
  skill_table <- paste0(format_lower, "_team_skill")

  # Check if table exists
  if (!skill_table %in% DBI::dbListTables(conn)) {
    cli::cli_alert_warning("Team skill table not found: {skill_table}")
    return(NULL)
  }

  query <- sprintf("
    SELECT *
    FROM %s
    WHERE LOWER(team_id) LIKE LOWER(?)
    ORDER BY delivery_id DESC
    LIMIT 1
  ", skill_table)

  team_skill <- DBI::dbGetQuery(conn, query, params = list(paste0("%", team_name, "%")))

  if (nrow(team_skill) == 0) {
    cli::cli_alert_warning("No team skill data found for {team_name}")
    return(NULL)
  }

  # Build data for plotting
  plot_data <- data.frame(
    Skill = c("Batting Runs", "Batting Wickets", "Bowling Runs", "Bowling Wickets"),
    Value = c(
      team_skill$batting_team_runs_skill,
      team_skill$batting_team_wicket_skill,
      team_skill$bowling_team_runs_skill,
      team_skill$bowling_team_wicket_skill
    ),
    Category = c("Batting", "Batting", "Bowling", "Bowling"),
    stringsAsFactors = FALSE
  )

  # Build plot
  p <- ggplot2::ggplot(plot_data, ggplot2::aes(
    x = Skill,
    y = Value,
    fill = Category
  )) +
    ggplot2::geom_col(width = 0.7) +
    ggplot2::geom_hline(yintercept = 0, color = "#333333") +
    ggplot2::geom_text(
      ggplot2::aes(label = sprintf("%+.3f", Value)),
      vjust = ifelse(plot_data$Value >= 0, -0.3, 1.3),
      size = 3.5
    ) +
    ggplot2::scale_fill_manual(values = c("Batting" = "#1E88E5", "Bowling" = "#E53935")) +
    ggplot2::coord_flip() +
    ggplot2::labs(
      title = paste(team_name, "- Team Skills"),
      subtitle = toupper(format),
      x = NULL,
      y = "Skill Index (deviation from expected)",
      caption = "Positive = better than average, Negative = worse"
    ) +
    theme_bouncer()

  return(p)
}
