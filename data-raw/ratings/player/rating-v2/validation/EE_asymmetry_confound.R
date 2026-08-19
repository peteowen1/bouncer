# Is the above/below asymmetry real, or just a ball-count confound?
#
# EB_symmetry.R measured the slope of a bridge player's reference deviation on
# his weak-competition deviation at 0.153 ABOVE the competition mean and 0.077
# BELOW -- a 2:1 gap, which would mean one compression multiplier is wrong for
# at least one side.
#
# THE CONFOUND: players who do badly get dropped, so below-mean records may
# simply be SHORTER. A shorter record is noisier, and regression attenuation
# scales with noise, so a lower slope below the mean could be pure measurement
# error rather than a property of weak competitions. That would make a
# two-sided compression term a fix for nothing.
#
# TEST: re-fit both slopes within strata of evidence. If the gap is attenuation
# it closes as evidence grows; if it is real it persists at every level.
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
         by = batter_id]
wk <- merge(pc[!comp %in% REF], rf, by = "batter_id")
wk <- merge(wk, off[, .(comp, m_here, m_ref, offset)], by = "comp")
wk <- wk[offset > 0]
wk[, `:=`(dev_weak = m - m_here, dev_ref = r_m - m_ref,
          w = 2 * balls * r_balls / (balls + r_balls))]

slope <- function(d) {
  if (nrow(d) < 25) return(c(NA_real_, nrow(d), NA_real_))
  f <- stats::lm(dev_ref ~ 0 + dev_weak, data = d, weights = d$w)
  se <- summary(f)$coefficients[1, 2]
  c(stats::coef(f)[[1]], nrow(d), se)
}

cat("=== does the above/below slope gap survive matching on evidence? ===\n")
cat("Evidence = harmonic weight of the bridge (balls in both, inverse-variance).\n")
cat("If the gap is a ball-count artefact it closes as evidence grows.\n\n")

# First: are below-mean records actually shorter? That is the confound itself.
cat(sprintf("median harmonic weight above the competition mean : %.0f (n = %d)\n",
            wk[dev_weak > 0, median(w)], wk[dev_weak > 0, .N]))
cat(sprintf("median harmonic weight below the competition mean : %.0f (n = %d)\n\n",
            wk[dev_weak < 0, median(w)], wk[dev_weak < 0, .N]))

wk[, wq := cut(w, stats::quantile(w, c(0, .25, .5, .75, 1)), include.lowest = TRUE,
               labels = c("Q1 thinnest", "Q2", "Q3", "Q4 thickest"))]
cat(sprintf("%-14s %8s %6s %8s %8s %6s %8s %8s\n",
            "evidence", "up", "n", "se", "down", "n", "se", "down/up"))
for (q in levels(wk$wq)) {
  u <- slope(wk[wq == q & dev_weak > 0]); d <- slope(wk[wq == q & dev_weak < 0])
  cat(sprintf("%-14s %8.3f %6d %8.3f %8.3f %6d %8.3f %8s\n", q,
      u[1], u[2], u[3], d[1], d[2], d[3],
      if (is.na(u[1]) || is.na(d[1])) "-" else sprintf("%.2f", d[1] / u[1])))
}
u <- slope(wk[dev_weak > 0]); d <- slope(wk[dev_weak < 0])
cat(sprintf("%-14s %8.3f %6d %8.3f %8.3f %6d %8.3f %8.2f\n", "ALL",
    u[1], u[2], u[3], d[1], d[2], d[3], d[1] / u[1]))

cat("\nAre the two slopes distinguishable at all? A formal interaction test,\n")
cat("which is what the eyeball comparison above is standing in for:\n")
wk[, below := as.integer(dev_weak < 0)]
fi <- stats::lm(dev_ref ~ 0 + dev_weak + dev_weak:below, data = wk, weights = wk$w)
co <- summary(fi)$coefficients
cat(sprintf("  interaction term %+.4f, se %.4f, t = %.2f, p = %.3f\n",
            co[2, 1], co[2, 2], co[2, 3], co[2, 4]))
cat(sprintf("  -> the sides are %s at the 5%% level\n",
            if (co[2, 4] < 0.05) "DISTINGUISHABLE" else "NOT distinguishable"))
