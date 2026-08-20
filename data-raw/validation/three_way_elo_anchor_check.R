# Does the 3-way batter run ELO rank T20 batters plausibly? (bouncerverse#63)
#
# The rebuild passing coverage, the leak anchor and separation says the table
# was BUILT correctly. It says nothing about whether the rating is any good,
# and #63 explicitly asks "including the possibility that it is still not worth
# having".
#
# Anchors chosen before looking: recognised elite T20 batters must not sit in
# the middle of a 2,178-player list. Measured 2026-08-20 on the fresh rebuild:
# Suryakumar Yadav 828th (5,718 balls), Babar Azam 1,077th and BELOW THE MEDIAN
# (8,039 balls), Buttler 803rd -- while Pat Cummins, a fast bowler, sits 10th.
#
# The exposure gradient is NOT the explanation and is checked here too: mean
# ELO rises with balls faced (1372 -> 1573) and spread narrows (164 -> 132),
# which is what a sane exposure response looks like. Both named batters sit
# below the mean of their OWN exposure bucket, so small-sample inflation does
# not account for them. The cause is not established; do not assume one.

# Why do elite high-volume batters sit mid-table while 400-ball players top it?
suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
conn <- get_db_connection(read_only = TRUE); on.exit(dbDisconnect(conn, shutdown=TRUE), add=TRUE)
d <- as.data.table(dbGetQuery(conn, "
  WITH bat AS (
    SELECT batter_id, batter_run_elo_after AS elo,
           COUNT(*) OVER (PARTITION BY batter_id) AS balls,
           ROW_NUMBER() OVER (PARTITION BY batter_id ORDER BY match_date DESC, delivery_id DESC) AS rn
    FROM main.mens_t20_3way_elo)
  SELECT batter_id, elo, balls FROM bat WHERE rn = 1"))
# DuckDB COUNT(*) comes back as integer64; cut() on it yields all-NA silently.
d[, balls := as.numeric(balls)]
d <- d[balls >= 200]
d[, dev := abs(elo - 1500)]
cat(sprintf("cor(balls, |elo-1500|) = %+.3f   (negative => small samples are the extreme ones)\n",
            cor(d$balls, d$dev)))
cat(sprintf("cor(balls, elo)         = %+.3f\n\n", cor(d$balls, d$elo)))
d[, bucket := cut(balls, c(200, 500, 1000, 2000, 5000, Inf),
                  labels = c("200-500","500-1k","1k-2k","2k-5k","5k+"), right = FALSE)]
d[, rk := frank(-elo)]            # rank once, over the whole table
print(d[, .(players = .N, mean_elo = round(mean(elo)), sd_elo = round(sd(elo)),
            mean_abs_dev = round(mean(dev)), in_top50 = sum(rk <= 50)),
        by = bucket][order(bucket)])
cat("\nIf the rating were exposure-aware, spread would NARROW with more balls,\n")
cat("not widen -- and the top 50 would not be dominated by the smallest samples.\n")
