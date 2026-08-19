# Is a weak competition's spread inflation SYMMETRIC?
#
# The shipped form divides a player's deviation from his competition's mean by
# the competition factor, in both directions. That is the right thing if a weak
# competition stretches the gaps between players equally above and below its
# own average. It is the WRONG thing if the stretch is top-heavy -- a few
# players smashing weak attacks while the poor ones are merely poor.
#
# It matters because the assumption is load-bearing at the bottom of the range:
# with m_here = 0.25, m_ref = 0.05 and cfactor = 1.6, a player scoring below
# -0.283 per ball is rated HIGHER for having done it in the weak competition
# than in the reference. That is the sign defect's own shape, one level down,
# and it is why the guard test in test-competition-adjust.R fails at negative
# values. Measure it rather than argue about it.
#
# METHOD: for each bridge player, his deviation from the weak competition's
# bridge mean, against his deviation from the reference mean among the same
# players. Slope estimated separately above and below the competition mean. A
# symmetric stretch gives the same slope on both sides.
suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
conn <- dbConnect(duckdb::duckdb(), dbdir = "C:/dev/bouncerverse/bouncerdata/bouncer.duckdb",
                  read_only = TRUE)
on.exit(dbDisconnect(conn, shutdown = TRUE), add = TRUE)

id_map <- build_player_id_map(conn)
b <- as.data.table(dbGetQuery(conn, sprintf("
  SELECT r.batter_id, r.bowler_id, r.raa, COALESCE(%s,'unknown') AS comp
  FROM main.cricsheet_ball_raa r JOIN cricsheet.matches m ON m.match_id = r.match_id
  WHERE r.format='T20' AND r.gender='male'", .competition_sql("t20"))))
canonicalise_player_ids(b, id_map)
eff <- fit_two_way_effects(b, prior_balls = 60, iterations = 20)
b[eff$bowler, on = "bowler_id", bowl_eff := i.eff][is.na(bowl_eff), bowl_eff := 0]
b[, v0 := raa - bowl_eff]
REF <- COMPETITION_REFERENCE_T20
off <- fit_competition_offsets(b, "batter_id", "v0", REF)

pc <- b[, .(balls = .N, m = mean(v0)), by = .(batter_id, comp)]
rf <- pc[comp %in% REF, .(r_balls = sum(balls), r_m = weighted.mean(m, balls)),
         by = batter_id][r_balls >= 50]
wk <- merge(pc[!comp %in% REF & balls >= 50], rf, by = "batter_id")
wk <- merge(wk, off[, .(comp, m_here, m_ref, offset)], by = "comp")
wk <- wk[offset > 0]                      # EASIER competitions only
wk[, `:=`(dev_weak = m - m_here, dev_ref = r_m - m_ref,
          w = 2 * balls * r_balls / (balls + r_balls))]

cat(sprintf("=== T20 men: %d bridge pairs across %d easier competitions ===\n\n",
            nrow(wk), uniqueN(wk$comp)))
fit <- function(d) {
  if (nrow(d) < 20) return(c(NA, NA))
  m <- stats::lm(dev_ref ~ 0 + dev_weak, data = d, weights = d$w)
  c(stats::coef(m)[[1]], nrow(d))
}
up <- fit(wk[dev_weak > 0]); dn <- fit(wk[dev_weak < 0])
cat("Slope of reference deviation on weak-competition deviation.\n")
cat("Slope 1 = no stretch. Slope below 1 = the weak competition stretches gaps,\n")
cat("so deviations must be COMPRESSED when mapped back.\n\n")
cat(sprintf("  above the competition mean : slope %.3f  (n = %d)\n", up[1], up[2]))
cat(sprintf("  below the competition mean : slope %.3f  (n = %d)\n", dn[1], dn[2]))
cat(sprintf("  ratio below/above          : %.2f\n", dn[1] / up[1]))
cat(sprintf("\n  1/median cfactor for these competitions, which is what the\n"))
fac <- fit_competition_factors(conn, "t20", "male", id_map = id_map)
cf <- merge(wk[, .N, by = comp], fac[, .(comp, factor)], by = "comp")
cat(sprintf("  shipped form currently applies to BOTH sides: %.3f\n", 1 / median(cf$factor)))

cat("\nSD of the two deviations, as a second read on the same question:\n")
cat(sprintf("  above: weak %.3f vs reference %.3f  (ratio %.2f)\n",
    wk[dev_weak > 0, sd(dev_weak)], wk[dev_weak > 0, sd(dev_ref)],
    wk[dev_weak > 0, sd(dev_weak) / sd(dev_ref)]))
cat(sprintf("  below: weak %.3f vs reference %.3f  (ratio %.2f)\n",
    wk[dev_weak < 0, sd(dev_weak)], wk[dev_weak < 0, sd(dev_ref)],
    wk[dev_weak < 0, sd(dev_weak) / sd(dev_ref)]))

cat("\nHow much of the pool sits below the crossover, where the shipped form\n")
cat("rates a weak-competition player ABOVE the same return in the reference:\n")
wk[, cross := m_ref + (m - m_here) / cf[match(comp, cf$comp), factor] > m]
cat(sprintf("  %d of %d bridge pairs (%.0f%%)\n",
    wk[cross == TRUE, .N], nrow(wk), 100 * wk[cross == TRUE, .N] / nrow(wk)))
