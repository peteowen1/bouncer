# Settle the above/below asymmetry properly.
#
# THE ERROR IN THE FIRST TWO ATTEMPTS: both EB_symmetry.R and
# EE_asymmetry_confound.R split players on the sign of `dev_weak` -- their
# deviation from the competition mean -- and then fitted a slope of the
# reference deviation on that same `dev_weak` within each half. That is
# conditioning on the noisy predictor and then regressing on it. Truncating a
# noisy regressor biases the within-group slope toward zero, by different
# amounts on each side depending on where the mass sits, so the 0.153-above /
# 0.077-below gap I reported was manufactured by the method. EE made it obvious
# by producing negative slopes in three of four evidence strata.
#
# THE FIX: classify on one half of the player's weak-competition record and
# MEASURE on the other. Classification noise is then independent of measurement
# noise, so the truncation no longer biases the slope being estimated.
#
# The split alternates over a stable sort rather than sampling, so it is
# deterministic without a seed -- a seeded split over an unordered query result
# reproduces nothing.
suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
conn <- dbConnect(duckdb::duckdb(), dbdir = file.path(find_bouncerdata_dir(), "bouncer.duckdb"),
                  read_only = TRUE)
on.exit(dbDisconnect(conn, shutdown = TRUE), add = TRUE)

id_map <- build_player_id_map(conn)
b <- as.data.table(dbGetQuery(conn, sprintf("
  SELECT r.delivery_id, r.batter_id, r.bowler_id, r.raa,
         COALESCE(%s,'unknown') AS comp
  FROM main.cricsheet_ball_raa r JOIN cricsheet.matches m ON m.match_id = r.match_id
  WHERE r.format='T20' AND r.gender='male'", .competition_sql("t20"))))
canonicalise_player_ids(b, id_map)
eff <- fit_two_way_effects(b, prior_balls = 60, iterations = 20)
b[eff$bowler, on = "bowler_id", bowl_eff := i.eff][is.na(bowl_eff), bowl_eff := 0]
b[, v0 := raa - bowl_eff]
REF <- COMPETITION_REFERENCE_T20
off <- fit_competition_offsets(b, "batter_id", "v0", REF)
easier <- off[offset > 0, comp]

rf <- b[comp %in% REF, .(r_balls = .N, r_m = mean(v0)), by = batter_id][r_balls >= 60]

# deterministic alternating split over a stable sort
setorder(b, batter_id, comp, delivery_id)
wk <- b[comp %in% easier]
wk[, half := seq_len(.N) %% 2L, by = .(batter_id, comp)]
sp <- dcast(wk[, .(n = .N, m = mean(v0)), by = .(batter_id, comp, half)],
            batter_id + comp ~ half, value.var = c("n", "m"))
setnames(sp, c("batter_id", "comp", "n_a", "n_b", "m_a", "m_b"))
sp <- sp[!is.na(m_a) & !is.na(m_b) & n_a >= 30 & n_b >= 30]
sp <- merge(sp, rf, by = "batter_id")
sp <- merge(sp, off[, .(comp, m_here, m_ref)], by = "comp")
sp[, `:=`(class_dev = m_a - m_here,        # classify on half A
          meas_dev  = m_b - m_here,        # measure on half B
          dev_ref   = r_m - m_ref)]
sp[, w := 2 * (n_a + n_b) * r_balls / ((n_a + n_b) + r_balls)]

cat(sprintf("=== T20 men: %d bridges with 30+ balls in EACH half and 60+ reference balls ===\n",
            nrow(sp)))
cat(sprintf("across %d easier competitions\n\n", uniqueN(sp$comp)))

slope <- function(d) {
  if (nrow(d) < 25) return(c(NA_real_, nrow(d), NA_real_))
  f <- stats::lm(dev_ref ~ 0 + meas_dev, data = d, weights = d$w)
  c(stats::coef(f)[[1]], nrow(d), summary(f)$coefficients[1, 2])
}
cat("Slope of reference deviation on the MEASUREMENT half, split by the\n")
cat("INDEPENDENT classification half. This is the comparison the earlier two\n")
cat("scripts were trying and failing to make.\n\n")
u <- slope(sp[class_dev > 0]); d <- slope(sp[class_dev < 0])
cat(sprintf("%-28s %8s %6s %8s\n", "classified by half A", "slope", "n", "se"))
cat(sprintf("%-28s %8.3f %6d %8.3f\n", "above the competition mean", u[1], u[2], u[3]))
cat(sprintf("%-28s %8.3f %6d %8.3f\n", "below the competition mean", d[1], d[2], d[3]))
if (!is.na(u[1]) && !is.na(d[1])) {
  diff <- u[1] - d[1]; se <- sqrt(u[3]^2 + d[3]^2)
  cat(sprintf("\ndifference %+.3f, se %.3f, z = %.2f, p = %.3f\n",
              diff, se, diff / se, 2 * stats::pnorm(-abs(diff / se))))
  cat(sprintf("-> the two sides are %s\n",
      if (2 * stats::pnorm(-abs(diff / se)) < 0.05) "DISTINGUISHABLE"
      else "NOT distinguishable: one multiplier is defensible"))
}

# The same question asked without any split at all, as a cross-check: does a
# quadratic term earn its place? If compression genuinely differs by side, the
# relationship is kinked and a squared term picks that up.
cat("\nCross-check without splitting -- does a quadratic term earn its place?\n")
f1 <- stats::lm(dev_ref ~ meas_dev, data = sp, weights = sp$w)
f2 <- stats::lm(dev_ref ~ meas_dev + I(meas_dev^2), data = sp, weights = sp$w)
an <- stats::anova(f1, f2)
cat(sprintf("  linear RSS %.2f, quadratic RSS %.2f, F = %.2f, p = %.3f\n",
            an$RSS[1], an$RSS[2], an$F[2], an$`Pr(>F)`[2]))
cat(sprintf("  -> a kinked relationship is %s\n",
    if (an$`Pr(>F)`[2] < 0.05) "SUPPORTED" else "NOT supported"))
