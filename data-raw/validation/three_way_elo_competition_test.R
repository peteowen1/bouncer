# Does the T20 3-way ELO penalise players for facing STRONGER opposition?
# (bouncerverse#63)
#
# The anchor failure is T20-only: Suryakumar Yadav 828th of 2,178 and Babar
# Azam 1,077th -- below the median -- while in ODI Kohli is 11th of 948. The
# hypothesis is that the T20 pool spans huge differences in competition
# strength (34.4 runs per 100 balls between the easiest and hardest buckets,
# D-P40/D-P42) and this ELO carries NO competition adjustment, so beating
# expectation in a weak league pays exactly what beating it in the IPL pays.
#
# THE TEST IS WITHIN-PLAYER, which is the point. Comparing strong-competition
# players against weak-competition players confounds skill with competition.
# Comparing the SAME player's residual across tiers does not: whatever his
# skill is, it is the same man in both rows.
#
# Residual = actual_runs - exp_runs, the quantity the ELO update is driven by.
# If the baseline is competition-blind, the same player should post a LOWER
# residual in the stronger tier -- and a player whose career sits mostly in
# that tier accumulates a lower rating for no reason but his schedule.
#
# ODI is run as the control. The hypothesis predicts a much smaller gap there,
# because that pool is almost all international and near-homogeneous.
suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})

MIN_BALLS_PER_TIER <- 200L
conn <- get_db_connection(read_only = TRUE)
on.exit(dbDisconnect(conn, shutdown = TRUE), add = TRUE)

top <- paste(sprintf("'%s'", COMPETITION_TOP_NATIONS), collapse = ", ")

run_format <- function(tbl, label) {
  d <- as.data.table(dbGetQuery(conn, sprintf("
    SELECT e.batter_id,
           (e.actual_runs - e.exp_runs) AS resid,
           CASE WHEN m.team_type = 'international'
                     AND m.team1 IN (%s) AND m.team2 IN (%s) THEN 'strong'
                WHEN m.event_name LIKE '%%Indian Premier League%%' THEN 'strong'
                ELSE 'rest' END AS tier
    FROM main.%s e JOIN cricsheet.matches m ON m.match_id = e.match_id",
    top, top, tbl)))

  agg <- d[, .(mean_resid = mean(resid), balls = .N), by = .(batter_id, tier)]
  w <- dcast(agg, batter_id ~ tier, value.var = c("mean_resid", "balls"))
  w <- w[balls_strong >= MIN_BALLS_PER_TIER & balls_rest >= MIN_BALLS_PER_TIER]
  w[, gap := mean_resid_strong - mean_resid_rest]

  cli::cli_h2(label)
  cli::cli_alert_info("{format(nrow(w), big.mark=',')} players with {MIN_BALLS_PER_TIER}+ balls in BOTH tiers")
  cli::cli_alert_info("mean residual: strong {round(mean(w$mean_resid_strong), 4)}, rest {round(mean(w$mean_resid_rest), 4)}")
  t <- t.test(w$gap)
  cli::cli_alert_info("within-player gap (strong - rest): {round(mean(w$gap), 4)} runs/ball, 95% CI [{round(t$conf.int[1],4)}, {round(t$conf.int[2],4)}], t = {round(t$statistic,1)}")
  cli::cli_alert_info("{round(100*mean(w$gap < 0), 1)}% of players post a LOWER residual in the stronger tier")
  invisible(mean(w$gap))
}

cli::cli_h1("Within-player residual by competition tier")
t20 <- run_format("mens_t20_3way_elo", "MENS T20")
odi <- run_format("mens_odi_3way_elo", "MENS ODI (control)")

cli::cli_h2("Verdict")
cat(sprintf("  T20 gap %+.4f runs/ball, ODI gap %+.4f runs/ball\n", t20, odi))
cat(sprintf("  Over a 2,000-ball career that is %+.0f runs of ELO drive in T20, %+.0f in ODI.\n",
            2000 * t20, 2000 * odi))
cat("\n  A negative T20 gap much larger than ODI's supports the hypothesis.\n")
cat("  A gap of similar size in both would refute it.\n")
