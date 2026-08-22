# "Who actually appeared" is NOT independent of the result (bouncerverse#61).
#
# #60 chose to compose the team rating from the players who actually appeared,
# because the XI is mostly unrecoverable. This measures what that choice costs.
#
# MEASURED 2026-08-22, male, decided matches:
#
#   format  cor(appearance-count difference, unified_margin)
#   T20                                              -0.558
#   ODI                                              -0.601
#   TEST                                             -0.111
#
# A side bowled out uses eleven batters; a side chasing comfortably uses four
# or five. So the COUNT of players who appear is a partial record of the
# result, and a rating SUMMED over appearances inherits it. The team rating is
# therefore a partly post-hoc feature: it knows something about the match it is
# being asked to predict.
#
# Two consequences, opposite in direction:
#
#   * It makes #61's NEGATIVE result stronger, not weaker. The rating had
#     access to outcome information and still lost to the result-ELO.
#   * It makes any FUTURE positive result suspect until the squad definition is
#     outcome-independent. A gain could be the leak rather than the skill.
#
# The fix is a squad definition fixed before the match -- a projected XI, which
# #60 considered and set aside. Using the MEAN rather than the SUM reduces the
# count sensitivity (see team_rating_count_sensitivity.R: T20 +0.579 -> +0.365)
# but does not remove it, because WHICH players appear is endogenous too.

# Does the number of players who APPEARED depend on what happened in the match?
# If it does, a rating summed over appearances carries the outcome it predicts.
suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
conn <- get_db_connection(read_only = TRUE); on.exit(dbDisconnect(conn, shutdown=TRUE), add=TRUE)
for (fmt in c("t20","odi","test")) {
  ty <- list(t20="'t20','it20'", odi="'odi','odm'", test="'test','mdm'")[[fmt]]
  d <- as.data.table(dbGetQuery(conn, sprintf("
    SELECT m.match_id, m.unified_margin,
           (SELECT MIN(batting_team) FROM cricsheet.match_innings i
            WHERE i.match_id=m.match_id AND i.innings=1) bat_first
    FROM cricsheet.matches m WHERE LOWER(m.match_type) IN (%s) AND m.gender='male'
      AND m.unified_margin IS NOT NULL AND m.unified_margin <> 0", ty)))
  d <- d[!is.na(bat_first)]
  app <- as.data.table(dbGetQuery(conn, sprintf("
    SELECT match_id, team, COUNT(DISTINCT player_id) n FROM (
      SELECT match_id, batting_team team, batter_id player_id FROM cricsheet.deliveries
      WHERE LOWER(match_type) IN (%s) AND gender='male'
      UNION
      SELECT match_id, bowling_team, bowler_id FROM cricsheet.deliveries
      WHERE LOWER(match_type) IN (%s) AND gender='male') GROUP BY 1,2", ty, ty)))
  x <- merge(d, app[, .(match_id, bat_first = team, n_bf = n)], by=c("match_id","bat_first"))
  ch <- app[, .(match_id, ch_team = team, n_ch = n)]
  x <- merge(x, ch, by="match_id", allow.cartesian=TRUE)[ch_team != bat_first]
  x[, n_diff := n_bf - n_ch]
  cat(sprintf("\n%s (%d matches)\n", toupper(fmt), nrow(x)))
  cat(sprintf("  cor(n_diff, unified_margin) = %+.3f   <- if non-zero, appearance COUNT knows the result\n",
              cor(x$n_diff, x$unified_margin)))
  cat(sprintf("  cor(n_batting_first, margin) = %+.3f\n", cor(x$n_bf, x$unified_margin)))
}
