# Home Venue Detection for Team ELO
#
# Functions for detecting home team advantage in cricket matches.
# Dual approach:
#   - Club teams: Match venue to team's most-played venue (mode)
#   - International: Match venue to country using hard-coded lookup


#' Build Home Venue Lookups
#'
#' Builds lookup tables for detecting home teams in matches.
#' Returns two lookups:
#' - club_home: Maps team_id to their home venue (mode venue)
#' - venue_country: Maps venue names to country names
#'
#' @param all_matches Data frame of matches with columns: team_type, team1, team2, venue, gender, match_type
#' @param format The format to use for team IDs: "t20", "odi", or "test".
#'   If NULL (default), derives format from match_type column.
#'
#' @return List with club_home (named character vector) and venue_country (named character vector)
#' @keywords internal
build_home_lookups <- function(all_matches, format = NULL) {

  # Format groupings for deriving format from match_type
  format_groups <- list(
    t20 = c("T20", "IT20"),
    odi = c("ODI", "ODM"),
    test = c("Test", "MDM")
  )

  # --- CLUB: team_id -> home_venue (mode venue) ---
  club_matches <- all_matches %>% dplyr::filter(.data$team_type == "club")

  if (nrow(club_matches) > 0) {
    club_home_venues <- club_matches %>%
      tidyr::pivot_longer(cols = c("team1", "team2"), names_to = "role", values_to = "team")

    # Derive format from match_type if not specified
    if (is.null(format)) {
      club_home_venues <- club_home_venues %>%
        dplyr::mutate(
          format = dplyr::case_when(
            .data$match_type %in% format_groups$t20 ~ "t20",
            .data$match_type %in% format_groups$odi ~ "odi",
            .data$match_type %in% format_groups$test ~ "test",
            TRUE ~ tolower(.data$match_type)
          ),
          team_id = make_team_id_vec(.data$team, .data$gender, .data$format, .data$team_type)
        )
    } else {
      club_home_venues <- club_home_venues %>%
        dplyr::mutate(team_id = make_team_id_vec(.data$team, .data$gender, format, .data$team_type))
    }

    club_home_venues <- club_home_venues %>%
      dplyr::filter(!is.na(.data$venue), .data$venue != "") %>%
      dplyr::group_by(.data$team_id, .data$venue) %>%
      dplyr::summarise(n_matches = dplyr::n(), .groups = "drop") %>%
      dplyr::group_by(.data$team_id) %>%
      dplyr::slice_max(.data$n_matches, n = 1, with_ties = FALSE) %>%
      dplyr::ungroup()

    club_home_lookup <- stats::setNames(club_home_venues$venue, club_home_venues$team_id)
  } else {
    club_home_lookup <- character(0)
  }

  # --- INTERNATIONAL: venue -> country ---
  venue_country_lookup <- build_venue_country_lookup(all_matches)

  list(
    club_home = club_home_lookup,
    venue_country = venue_country_lookup
  )
}


#' Detect Home Team
#'
#' Determines which team (if any) is playing at home.
#' Uses dual lookup approach:
#' - Club matches: Check if venue matches team's home ground
#' - International: Check if venue is in team's country
#'
#' @param team1 Name of team 1
#' @param team2 Name of team 2
#' @param team1_id Composite ID of team 1
#' @param team2_id Composite ID of team 2
#' @param venue Venue name
#' @param team_type Either "club" or "international"
#' @param club_home_lookup Named vector mapping team_id to home venue
#' @param venue_country_lookup Named vector mapping venue to country
#'
#' @return Integer: 1 if team1 is home, -1 if team2 is home, 0 if neutral/unknown
#' @keywords internal
detect_home_team <- function(team1, team2, team1_id, team2_id, venue, team_type,
                             club_home_lookup, venue_country_lookup) {
  if (is.na(venue) || venue == "") return(0L)

  if (team_type == "club") {
    # Club: Check if venue matches either team's home ground
    # Use safe lookup with match() to avoid subscript out of bounds
    t1_idx <- match(team1_id, names(club_home_lookup))
    t2_idx <- match(team2_id, names(club_home_lookup))

    t1_home <- if (!is.na(t1_idx)) club_home_lookup[t1_idx] else NA_character_
    t2_home <- if (!is.na(t2_idx)) club_home_lookup[t2_idx] else NA_character_

    if (!is.na(t1_home) && venue == t1_home) return(1L)
    if (!is.na(t2_home) && venue == t2_home) return(-1L)

  } else {
    # International: Check which country the venue is in
    venue_idx <- match(venue, names(venue_country_lookup))
    venue_country <- if (!is.na(venue_idx)) venue_country_lookup[venue_idx] else NA_character_

    if (!is.na(venue_country)) {
      # Match country to team name (exact match)
      if (venue_country == team1) return(1L)
      if (venue_country == team2) return(-1L)
    }
  }

  return(0L)  # Neutral
}


# Session-level cache for venue country map
.venue_country_cache <- new.env(parent = emptyenv())

#' Get Venue to Country Mapping
#'
#' Returns a named list mapping international cricket venue names to country names.
#' Loaded from inst/extdata/venue_country_map.csv and cached for the session.
#' Country names match team names as they appear in the data (e.g., "West Indies" not "Barbados").
#'
#' @return Named list where names are venue names and values are country names
#' @keywords internal
get_venue_country_map <- function() {
  if (exists("map", envir = .venue_country_cache)) {
    return(get("map", envir = .venue_country_cache))
  }

  csv_path <- system.file("extdata", "venue_country_map.csv", package = "bouncer")
  if (csv_path == "") {
    cli::cli_abort("venue_country_map.csv not found in package inst/extdata/")
  }

  df <- utils::read.csv(csv_path, stringsAsFactors = FALSE)
  if (!all(c("venue", "country") %in% names(df))) {
    cli::cli_abort("venue_country_map.csv must have 'venue' and 'country' columns, found: {paste(names(df), collapse=', ')}")
  }
  if (nrow(df) == 0) {
    cli::cli_abort("venue_country_map.csv is empty")
  }
  venue_map <- stats::setNames(as.list(df$country), df$venue)
  assign("map", venue_map, envir = .venue_country_cache)
  venue_map
}

# Legacy function body removed — data now in inst/extdata/venue_country_map.csv
# To add venues, edit the CSV file directly.


#' Build Venue to Country Lookup for International Matches
#'
#' Creates a lookup table mapping venue names to country names for international matches.
#' Uses the hard-coded venue map from \code{\link{get_venue_country_map}}, with a fallback
#' to mode-based detection (team that plays there most) for unknown venues.
#'
#' @param all_matches Data frame of matches with columns: team_type, venue, team1, team2
#'
#' @return Named character vector where names are venue names and values are country names
#' @keywords internal
build_venue_country_lookup <- function(all_matches) {
  # Get unique venues from international matches
  intl_matches <- all_matches %>% dplyr::filter(.data$team_type == "international")
  if (nrow(intl_matches) == 0) return(character(0))

  unique_venues <- unique(intl_matches$venue)
  unique_venues <- unique_venues[!is.na(unique_venues) & unique_venues != ""]

  # Get the hard-coded venue map

  venue_country_map <- get_venue_country_map()

  # Pre-lowercase map keys for O(1) lookup instead of O(n) scan
  lower_map <- stats::setNames(
    as.character(venue_country_map),
    tolower(names(venue_country_map))
  )

  # Build lookup from hard-coded map
  countries <- lower_map[tolower(unique_venues)]
  names(countries) <- unique_venues
  venue_country_lookup <- countries[!is.na(countries)]

  # Fallback: for unmatched venues, use mode-based approach (team that plays there most)
  unmatched_venues <- unique_venues[is.na(countries)]

  if (length(unmatched_venues) > 0) {
    cli::cli_alert_info("Found {length(unmatched_venues)} venues not in hard-coded map, using mode fallback")

    # Find which team plays at each unmatched venue most often
    mode_venue_data <- intl_matches %>%
      dplyr::filter(.data$venue %in% unmatched_venues) %>%
      tidyr::pivot_longer(cols = c("team1", "team2"), names_to = "role", values_to = "team") %>%
      dplyr::group_by(.data$venue, .data$team) %>%
      dplyr::summarise(n_matches = dplyr::n(), .groups = "drop") %>%
      dplyr::group_by(.data$venue) %>%
      dplyr::slice_max(.data$n_matches, n = 1, with_ties = FALSE) %>%
      dplyr::ungroup()

    # Add to lookup (mode-based fallback)
    mode_lookup <- stats::setNames(mode_venue_data$team, mode_venue_data$venue)
    venue_country_lookup <- c(venue_country_lookup, mode_lookup)
  }

  venue_country_lookup
}
