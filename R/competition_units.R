# Normalised competition units for the men's Test + first-class pool.
#
# Decision record: .scratch/player-delivery-elo/issues/11-define-the-competition-unit.md
# (bouncerverse repo). Raw `event_name` is NOT a usable competition key: Test cricket is
# split across 174 bilateral series names over 877 matches, and English county cricket is
# split three ways by sponsor.
#
# Six units, partitioning all 5,221,238 deliveries exactly.

#' Competition Unit Lookup
#'
#' Maps a Cricsheet `event_name` to a normalised competition unit. Test cricket collapses
#' to a single unit regardless of series name; conditions are owned by the venue layer,
#' not by competition (see the decision record).
#'
#' @format A named character vector: names are `event_name` values, values are units.
#' @export
COMPETITION_UNIT_MAP <- c(
  # English county cricket -- sponsor names for one continuous competition.
  # Bob Willis Trophy is the 2020 COVID replacement; without it English first-class
  # cricket has a hole in 2020, so it maps in. Flagged as a reduced season below.
  "County Championship"            = "County Championship",
  "Specsavers County Championship" = "County Championship",
  "LV= County Championship"        = "County Championship",
  "Bob Willis Trophy"              = "County Championship",
  # Australian domestic -- "Marsh" is a sponsor name covering the 2019/20 season only.
  "Sheffield Shield"               = "Sheffield Shield",
  "Marsh Sheffield Shield"         = "Sheffield Shield",
  # New Zealand domestic -- single name throughout.
  "Plunket Shield"                 = "Plunket Shield",
  # Sri Lankan domestic first-class, 18 clubs, 2024-12 onward only.
  "Major League Tournament"        = "SL Major League Tournament",
  # Associate-nation first-class. Cup and Shield are one programme.
  "ICC Intercontinental Cup"       = "ICC Intercontinental",
  "ICC Intercontinental Shield"    = "ICC Intercontinental"
)

#' Seasons Where a Competition Ran in a Reduced Format
#'
#' Strength estimates for these are less comparable to the competition's other seasons.
#' Report them, do not silently drop them.
#' @export
COMPETITION_REDUCED_SEASONS <- list(
  "County Championship" = c("2020", "2021")  # Bob Willis Trophy: 13 and 1 matches
)

#' Normalise a Competition
#'
#' @param event_name Character vector of Cricsheet `event_name` values (NA allowed).
#' @param match_type Character vector of `match_type` values. Any `"Test"` row returns
#'   `"Test"` regardless of `event_name` -- the 174 series names are not competitions.
#' @return Character vector of normalised competition units. Unrecognised first-class
#'   events return `NA` rather than a guess, so new competitions surface loudly.
#' @examples
#' normalise_competition(c("The Ashes", "LV= County Championship"), c("Test", "MDM"))
#' @export
normalise_competition <- function(event_name, match_type) {
  stopifnot(length(event_name) == length(match_type))
  out <- unname(COMPETITION_UNIT_MAP[as.character(event_name)])
  out[match_type == "Test"] <- "Test"
  out
}
