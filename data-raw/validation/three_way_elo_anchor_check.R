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
#
# WHAT IS ESTABLISHED, from code rather than guesswork (2026-08-20):
#
# The package HAS exposure shrinkage -- calculate_reliability() and
# blend_elo_with_replacement() in R/three_way_elo.R, reliability =
# balls / (balls + halflife) -- and the production path NEVER CALLS IT. The
# only callers are optimization/05_validate_3way_elo.R and the unit tests.
# 01_calculate_3way_elo.R stores the raw ELO, calculate_roster_elo() reads the
# raw value, and 02_train_full_model.R joins the raw *_elo_before columns. So
# both the published rating and the model feature are unshrunk.
#
# That accounts for one half of the failure and not the other:
#
#   * IT EXPLAINS the top of the list. Dhruv Jurel sits at the 1800 ceiling on
#     545 balls, Shashank Singh on 813, Pat Cummins -- a fast bowler -- ranks
#     10th on 700. Low exposure, extreme rating, no shrinkage.
#
#   * IT DOES NOT EXPLAIN Suryakumar Yadav 828th on 5,718 balls or Babar Azam
#     1,077th on 8,039. Blending moves a rating TOWARD replacement, so it would
#     push them DOWN, not up. Whatever rates them below the mean of their own
#     exposure bucket is a separate mechanism and is still unidentified.
#
# Do not "fix" this by switching the blend on and declaring victory: it would
# tidy the ceiling while leaving the harder failure untouched, and the tidier
# list would read as though both were solved.
#
# THE FORMAT CONTRAST, measured 2026-08-20 on the fresh ODI rebuild, separates
# the two halves cleanly:
#
#   MENS ODI, 948 batters at 500+ balls: Kohli 11th (15,958 balls), Warner
#   37th, Root 50th, Rohit 131st, Gill 166th, Babar 354th. A defensible
#   ordering. Yet its top 10 still holds Simon Harmer -- a SPIN BOWLER -- at
#   number one on 628 balls, and David Willey, another bowler, at five.
#
# So the two failures are separable, and only one is format-specific:
#
#   * the low-exposure ceiling appears in BOTH formats, which is what a missing
#     shrinkage step predicts;
#   * elite batters buried mid-table is T20-ONLY. It has no ODI counterpart.
#
# HYPOTHESIS, not a finding -- untested as of this writing. The T20 pool spans
# wildly different competition strengths (D-P40/D-P42 and
# docs/reference/COMPETITION-MATRIX.md: 34.4 runs per 100 balls between the
# easiest and hardest buckets), and this ELO carries NO competition adjustment,
# so beating expectation in a weak league pays exactly what beating it in the
# IPL pays. Suryakumar Yadav and Babar Azam play most of their T20 cricket in
# the strongest competitions. The ODI pool is almost entirely international and
# near-homogeneous, which is where such an effect would vanish. The format
# contrast is evidence for this, not proof of it. Test before acting.
#
# ANSWERED 2026-08-20, and it was neither hypothesis. The T20 anchor failure
# was substantially the PLAYER IDENTITY REGRESSION (#74): for essentially all
# of 2026 the corpus filed players under a NAME instead of a registry id, which
# split every current player in two and put 3,139 phantom low-exposure
# identities into the pool -- as batters, and as the BOWLERS and venues the
# ratings are computed against.
#
# Rebuilt on the corrected corpus, same code, same parameters:
#
#   batter        rank before -> after   balls
#   JC Buttler          803 ->    2      9,410
#   Babar Azam        1,077 ->   79      8,455
#   V Kohli             406 ->   59     10,134
#   GJ Maxwell          511 ->  187      6,996
#   SA Yadav            828 ->  260      5,949
#   TM Head              51 ->  100      3,387
#   RG Sharma           263 ->  617      8,608
#
# The ceiling cleared too: 3 players at 1800 against 5, four within ten of it
# against twelve, and the 200-500 ball bucket's share of the top 50 halved.
# The exposure gradient is now monotonic -- mean 1350 -> 1568 across buckets,
# spread narrowing 156 -> 125.
#
# TWO THINGS STILL WORTH WATCHING, so this does not read as fully solved:
#   * Rohit Sharma moved the WRONG way, 263 -> 617 on 8,608 balls. One anchor
#     regressing while six improve is not proof of anything, but it is not
#     nothing either.
#   * Suryakumar Yadav at 260 is defensible rather than obviously right.
#   * blend_elo_with_replacement() is STILL never called. The ceiling improved
#     because the phantoms left the pool, not because shrinkage was switched
#     on. That remains available and untested.

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
