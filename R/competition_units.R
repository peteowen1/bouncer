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

# Bilateral tours and short series -------------------------------------------
#
# Cricsheet names every bilateral series as its own event: "Zimbabwe in New
# Zealand T20I Series", "Gibraltar tour of Malta", "Bulgaria Tri-Nation T20I
# Series". Fitting a competition factor per name means fitting 326 separate
# strengths off a median of 5 matches each, and the result is noise wearing the
# label of league strength:
#
#   * "Zimbabwe in New Zealand T20I Series" -- Williamson, McCullum, Guptill,
#     Taylor -- came out as the weakest competition in the fit at that point,
#     2.90. (After the first pass of grouping, a bare one-match "Ireland v
#     Zimbabwe" took that title at the 4.0 clamp ceiling -- see the pattern
#     notes below. Two different series held it at two different stages; neither
#     is "the weakest on record" without saying when.)
#   * "Australia tour of Bangladesh", 5 matches, came out as the HARDEST
#     competition on record at 0.53 -- harder than the IPL or the World Cup.
#   * 88 of 267 short international events rated weaker than 2.0, and 35 were
#     pinned at the 4.0 ceiling.
#
# What those factors actually captured was the pitch and conditions of a handful
# of matches. Grouping the series into a few large units gives each a real
# sample. Named tournaments (ICC events, qualifiers, continental cups) are NOT
# affected -- they are separately named and keep their own factors.
#
# FOUR buckets rather than one, because a Zimbabwe-New Zealand series and a
# Gibraltar-Malta series are not the same standard: Top Nations, Mixed,
# Associate and Developing. Membership crosses two lists -- COMPETITION_TOP_NATIONS
# and COMPETITION_WC_ASSOCIATES -- and is NOT decided by ICC Full Member status;
# see the note on COMPETITION_TOP_NATIONS immediately below, which deliberately
# excludes two Full Members.
COMPETITION_TOP_NATIONS <- c(
  "Afghanistan", "Australia", "Bangladesh", "England", "India",
  "New Zealand", "Pakistan", "South Africa", "Sri Lanka", "West Indies"
)
# NOTE: this is a PLAYING-STANDARD list, not the ICC Full Member list. Ireland
# and Zimbabwe are Full Members (Test status 2017 and 1992) and are deliberately
# NOT here: Pete's call, on the basis that a Zimbabwe-Ireland series is closer in
# standard to Netherlands-Scotland than to India-Australia. Two attempts to settle
# it from the data were both confounded and neither was used --
#   * mean RAA conceded ranks Associates ABOVE Full Members, because the agnostic
#     model already carries league_avg_runs and event_tier, so RAA is measured
#     against a baseline that is already raised for weak competitions;
#   * raw runs conceded per ball ranks the Test 9 WORSE (1.271 vs 1.111), because
#     they bowl at stronger batting sides -- that measures opposition, not self.
#
# DATA GAP: Afghanistan is listed for when the data is fixed, but is currently
# ABSENT FROM THE ENTIRE DATABASE -- zero balls in any format, while every other
# Full Member has 30-51k balls of international T20 alone. They were 2024 T20
# World Cup semi-finalists. Their absence is why Rashid Khan, Mujeeb and Noor
# Ahmad show no country on the leaderboards. This is a bouncerdata collection
# gap, not a modelling choice.

#' SQL fragment that recognises a bilateral tour or short multi-team series
#'
#' Matches the naming shapes cricsheet uses. Kept as one definition so the
#' rating and the diagnostics cannot drift apart.
#' @keywords internal
COMPETITION_TOUR_PATTERN_SQL <- paste(
  # NOTE: DuckDB's SIMILAR TO is RE2 regex, where '%' is a LITERAL character and
  # the pattern must match the whole string -- it is NOT a LIKE wildcard. An
  # earlier version used SIMILAR TO here and silently matched nothing, leaving
  # "Zimbabwe in New Zealand T20I Series" as its own competition. Use LIKE for
  # wildcards and regexp_matches() for alternation.
  "m.event_name LIKE '%% tour of %%'",
  # Anything cricsheet calls a "Series" and plays between national sides is a
  # bilateral or short multi-team series, never a standing tournament. This one
  # line catches "Hong Kong Men's T20I Series", "No Frills T20I Series",
  # "Pearl of Africa T20I Series" and the rest that the shapes below miss.
  "OR m.event_name LIKE '%%Series'",
  "OR regexp_matches(m.event_name, ' in .*(Series|T20I|ODI|Twenty20)')",
  "OR regexp_matches(m.event_name, ' v .*(Series|T20I|ODI|Twenty20)')",
  # A bare "Ireland v Zimbabwe" or "Ireland vs South Africa" -- two team names
  # and nothing else. One match, three bridge players, and after the first pass
  # of grouping it was the weakest competition in the fit, pinned at the 4.0
  # clamp ceiling.
  "OR regexp_matches(m.event_name, '^[A-Za-z][A-Za-z ]* vs? [A-Za-z][A-Za-z ]*$')",
  "OR regexp_matches(m.event_name, '(Tri-Nation|Tri-Series|Triangular|Quadrangular|Pentangular)')",
  sep = " ")

#' SQL fragment recognising the ICC qualifying pathway
#'
#' The World Cup qualifying ladder is 49 separately-named events in the men's
#' T20 data alone -- regional qualifiers, sub-regional groups, divisions,
#' World Cricket League stages -- eight of them with six matches or fewer, and
#' several pinned at the 4.0 factor ceiling off a handful of bridge players.
#' They are one competitive standard: associate nations playing for World Cup
#' places. Fitting one factor across all of them uses 227,895 balls instead of
#' a few hundred. The World Cup proper is NOT in here -- it is a separate,
#' substantial tournament and keeps its own factor.
#' @keywords internal
COMPETITION_PATHWAY_PATTERN_SQL <- paste(
  "m.event_name NOT LIKE '%%World Cup'",
  "AND regexp_matches(m.event_name,",
  "'(Qualifier|Region|Division|World Cricket League|Challenge League|Pre-Qualifier)')",
  sep = " ")

#' Nations that have reached a T20 World Cup without being a top-10 side
#'
#' Derived from the data: teams appearing in "ICC Men's T20 World Cup",
#' "ICC World Twenty20" or "World T20" and not in [COMPETITION_TOP_NATIONS].
#' Reaching a World Cup is an observable qualification bar, so this line is a
#' fact about results rather than a judgement about reputation.
#'
#' It exists because one "Other Nations" bucket was too coarse. Netherlands and
#' Scotland sat on the same 1.25 factor as Malta and Gibraltar, and the visible
#' consequence was Karanbir Singh ranking 3rd among T20 men on 1,354 balls at an
#' average of 58.7 in European associate cricket.
#' @keywords internal
COMPETITION_WC_ASSOCIATES <- c(
  "Ireland", "Netherlands", "Zimbabwe", "Scotland", "Namibia", "Oman",
  "United States of America", "United Arab Emirates", "Nepal", "Canada",
  "Papua New Guinea", "Hong Kong", "Italy", "Uganda", "Kenya"
)

