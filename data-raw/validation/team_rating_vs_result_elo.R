# Does a team rating built from player ratings beat the result-ELO baseline?
# (bouncerverse#61)
#
# THE BAR, from the ticket: held-out match prediction, per format, with the
# MATCH as the independent unit. Not a plausible-looking table.
#
# ############ THE LEAK THIS SHAPE INVITES, READ BEFORE TRUSTING ANY NUMBER
#
# main.player_value_v2 is a SNAPSHOT with a single `as_at` date. It was fitted
# on the whole corpus, including the matches this script would score. Using it
# to "predict" those matches means the rating already contains their outcome:
# a player's value is high partly BECAUSE of the match being predicted.
#
# That is not a subtle risk. It is the same defect as the venue features that
# were the label at one-match venues (#29, #69), and it would produce a large,
# entirely convincing improvement over the result-ELO -- which updates strictly
# forward in time and therefore cannot cheat.
#
# So this script REFUSES to report a comparison unless the ratings are
# time-causal with respect to each match it scores. Two ways to satisfy it:
#
#   (a) restrict scoring to matches AFTER the ratings' as_at date -- honest,
#       but leaves very few matches; or
#   (b) rebuild ratings as-at each match date (expanding window), which is what
#       a real answer needs and is a substantial job.
#
# Until one of those exists, the honest output of this script is a refusal,
# not a number. A refusal is a result; a leaked 20% improvement is not.
# ###########################################################################
suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})

conn <- get_db_connection(read_only = TRUE)
on.exit(dbDisconnect(conn, shutdown = TRUE), add = TRUE)

cli::cli_h1("Team rating vs result-ELO")

as_at <- dbGetQuery(conn, "SELECT DISTINCT as_at FROM main.player_value_v2 ORDER BY 1")$as_at
cli::cli_alert_info("player_value_v2 as_at dates: {length(as_at)} ({paste(utils::head(as_at, 3), collapse = ', ')})")

if (length(as_at) <= 1) {
  latest <- max(as.Date(as_at))
  n_after <- dbGetQuery(conn, sprintf("
    SELECT COUNT(*) AS n FROM cricsheet.matches WHERE match_date > DATE '%s'", latest))$n
  cli::cli_abort(c(
    "player_value_v2 is a SINGLE snapshot as at {latest} -- it is not time-causal.",
    "x" = "Scoring matches on or before that date asks a rating that already contains their outcome to predict them.",
    "i" = "Matches strictly after it: {n_after}. That is the only honestly scorable set from this snapshot.",
    "i" = "A real answer needs ratings rebuilt as-at each match date (expanding window).",
    "i" = "Refusing rather than reporting a number: see #29 and #69, where a feature that WAS the label correlated 1.000 with the outcome."))
}

cli::cli_alert_success("Ratings carry {length(as_at)} distinct as_at dates; per-match causality is checkable.")
