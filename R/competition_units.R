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

#' Sponsor and Naming Aliases for Limited-Overs Competitions
#'
#' A competition that changes sponsor changes its `event_name` while remaining
#' the same competition. Fitting a difficulty factor per name estimates separate
#' strengths for synonyms and splits every bridge player between them -- the
#' same defect [COMPETITION_UNIT_MAP] fixes for first-class cricket, which had
#' gone unnoticed in the limited-overs buckets.
#'
#' Measured 2026-08-18, matches split across names:
#' \itemize{
#'   \item England domestic T20 -- **1,554** matches across three names, more
#'     than the IPL's 1,243
#'   \item England domestic 50-over -- **832** across two
#'   \item Men's T20 World Cup -- 334 across three
#'   \item South Africa domestic T20 -- 314 across three
#'   \item England women's domestic 50-over -- 282 across two
#'   \item Men's Cricket World Cup -- 229 across two
#' }
#'
#' Canonical form is the CURRENT name, so a reference set written today keeps
#' working as sponsors change. Unlike [COMPETITION_UNIT_MAP] this is a rename
#' map, not a partition: an event not listed here passes through unchanged,
#' because limited-overs cricket has hundreds of genuinely distinct competitions
#' and they cannot be enumerated.
#'
#' @format Named character vector: names are variant `event_name`s, values the
#'   canonical name.
#' @export
COMPETITION_ALIASES <- c(
  # England domestic T20: NatWest -> Vitality, and "Men" appended from 2025.
  "NatWest T20 Blast"                    = "Vitality Blast",
  "Vitality Blast Men"                   = "Vitality Blast",
  # South Africa domestic T20, three sponsors of one competition.
  "MiWAY T20 Challenge"                  = "CSA T20 Challenge",
  "Ram Slam T20 Challenge"               = "CSA T20 Challenge",
  # The men's global T20 event, renamed twice.
  "ICC World Twenty20"                   = "ICC Men's T20 World Cup",
  "World T20"                            = "ICC Men's T20 World Cup",
  "ICC World Twenty20 Qualifier"         = "ICC Men's T20 World Cup Qualifier",
  # The women's global T20 event.
  "Women's World T20"                    = "ICC Women's T20 World Cup",
  # England domestic 50-over: Royal London sponsorship ended after 2022.
  "Royal London One-Day Cup"             = "One-Day Cup",
  # England women's domestic 50-over. The RHF Trophy ran 2020-2024 and its
  # successor carries the ECB name -- note this one also silently broke
  # COMPETITION_REFERENCE_ODI_FEMALE, which anchored on the retired name.
  "Rachael Heyhoe Flint Trophy"          = "ECB Women's One-Day Cup",
  # Australia domestic 50-over. Marsh sponsorship, then the plain name.
  "The Marsh Cup"                        = "One-Day Cup (Australia)",
  # The men's global 50-over event.
  "ICC World Cup"                        = "ICC Cricket World Cup"
)

#' Apply Limited-Overs Competition Aliases
#'
#' @param event_name Character vector of `event_name` values.
#' @return The same vector with sponsor variants mapped to their canonical
#'   name; anything unlisted is returned unchanged, and `NA` stays `NA`.
#' @examples
#' alias_competition(c("NatWest T20 Blast", "Indian Premier League"))
#' @export
alias_competition <- function(event_name) {
  out <- unname(COMPETITION_ALIASES[as.character(event_name)])
  ifelse(is.na(out), as.character(event_name), out)
}
