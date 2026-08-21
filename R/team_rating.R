# A team rating built from player ratings (bouncerverse#60, #61).
#
# WHY NOT A SUM. player_value_v2.total_value is bat_value + bowl_value, and on
# genuine all-rounders (200+ balls both ways) batting is 37.0% of the summed
# variance in T20, 19.7% in ODI and 7.6% in Test -- where the composite
# correlates -0.085 with batting. Summing gives a bowling-only team rating
# wearing the name of a complete one. Same shape as D-P7, where EPR's WPA term
# was 0.009% of its variance and every WPA improvement was therefore inert.
#
# The imbalance is an EXPOSURE artefact before it is a skill statement: a Test
# bowler bowls far more deliveries than a Test batter faces, so an accumulated
# runs-above-average figure grows faster for bowling regardless of who is
# better. Putting both on runs PER STANDARD MATCH removes that, and is the
# composition #60 settled on.
#
# WHAT IT CANNOT DO, by construction. #60 chose "rate who actually appeared",
# because the XI is mostly not recoverable -- T20 deliveries show 15.8 batters
# and 11.9 bowlers of 22. That makes this rating RETROSPECTIVE: it cannot be
# computed before a match, because nobody has appeared yet. Simulation and
# pre-match prediction need a separate selection step, and that step's error is
# not measured by anything here.

#' Standard per-match exposure, by format
#'
#' Balls faced by a batter and bowled by a bowler in a typical match. Used to
#' put accumulated value on a per-match scale so batting and bowling are
#' comparable. Not tuning knobs -- change them only against measured medians.
#' @keywords internal
TEAM_RATING_EXPOSURE <- list(
  t20  = c(bat = 20, bowl = 24),
  odi  = c(bat = 40, bowl = 60),
  test = c(bat = 70, bowl = 120)
)

#' Put Batting and Bowling Value on a Common Runs-Per-Match Scale
#'
#' @param value Numeric. Accumulated value (runs above average).
#' @param balls Numeric. Balls over which it accumulated.
#' @param format Character.
#' @param role `"bat"` or `"bowl"`.
#' @return Runs above average per standard match. `NA` where `balls` is 0 --
#'   deliberately, because a player with no exposure has no measured value and
#'   filling that with 0 would say "exactly average", which is a claim.
#' @keywords internal
value_per_match <- function(value, balls, format, role) {
  fmt <- tolower(format[1])
  if (!fmt %in% names(TEAM_RATING_EXPOSURE)) {
    cli::cli_abort("No standard exposure for format {.val {fmt}}.")
  }
  std <- TEAM_RATING_EXPOSURE[[fmt]][[match.arg(role, c("bat", "bowl"))]]
  out <- ifelse(balls > 0, value / balls * std, NA_real_)
  out
}

#' Assert the Composition Has Not Collapsed Into One Component
#'
#' Anchor 5 from #60, declared before any rating was computed: neither batting
#' nor bowling may fall below `min_share` of the composite's variance. Below
#' that the team rating is a one-sided rating that still looks complete, which
#' is the exact failure the design exists to avoid and which nothing else in
#' the pipeline would surface.
#'
#' @param bat,bowl Numeric vectors, already on a common scale.
#' @param min_share Numeric. Floor on either component's variance share.
#' @keywords internal
assert_component_balance <- function(bat, bowl, min_share = 0.15) {
  ok <- stats::complete.cases(bat, bowl)
  if (sum(ok) < 30) cli::cli_abort("Only {sum(ok)} complete pairs; cannot judge balance.")
  vb <- stats::var(bat[ok]); vw <- stats::var(bowl[ok])
  share_bat <- vb / (vb + vw)
  if (share_bat < min_share || (1 - share_bat) < min_share) {
    cli::cli_abort(c(
      "Composition has collapsed: batting is {round(100*share_bat, 1)}% of the variance.",
      "x" = "Anchor 5 (#60) requires each component to hold at least {round(100*min_share)}%.",
      "i" = "A one-sided rating that still looks complete is what this check exists to catch."))
  }
  invisible(c(bat = share_bat, bowl = 1 - share_bat))
}
