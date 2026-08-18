# D-P32: the team-score model and lambda disagree on an ODI wicket (14.3 vs 23.0).
# Measure the MARGINAL cost of a wicket the same way in both formats: within
# matched (over, wickets-down) states, mean TSA on wicket balls minus mean TSA on
# 0-run non-wicket balls. Comparing a wicket to a dot isolates the dismissal,
# because both score zero runs.
suppressMessages({library(DBI); library(data.table)})
conn <- DBI::dbConnect(duckdb::duckdb(), dbdir="C:/dev/bouncerverse/bouncerdata/bouncer.duckdb", read_only=TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown=TRUE), add=TRUE)

for (fmt in c("T20","ODI")) {
  lam <- if (fmt=="T20") 9.0 else 23.0
  d <- as.data.table(DBI::dbGetQuery(conn, sprintf("
    SELECT w.delta_ps AS tsa, r.is_wicket, r.actual_runs, r.over_number,
           r.innings_number, dl.wickets_fallen - CAST(dl.is_wicket AS INT) AS wpre
    FROM main.cricsheet_ball_win_probability w
    JOIN main.cricsheet_ball_raa r ON r.delivery_id = w.delivery_id
    JOIN cricsheet.deliveries dl ON dl.delivery_id = w.delivery_id
    WHERE r.format='%s' AND r.gender='male' AND w.delta_ps IS NOT NULL
      AND r.actual_runs = 0", fmt)))
  cat(sprintf("\n================ %s men, innings 1+2, %s zero-run deliveries ================\n",
      fmt, format(nrow(d), big.mark=",")))
  # matched-state difference
  d[, cell := paste(over_number, pmin(wpre,9), innings_number)]
  agg <- d[, .(nw = sum(is_wicket==1), nd = sum(is_wicket==0),
               mw = mean(tsa[is_wicket==1]), md = mean(tsa[is_wicket==0])), by=cell]
  agg <- agg[nw >= 5 & nd >= 20]
  agg[, diff := mw - md]
  # weight each state by how many wickets actually fell there
  cost <- agg[, sum(diff*nw)/sum(nw)]
  cat(sprintf("  matched states used: %s (>=5 wickets and >=20 dots each)\n", nrow(agg)))
  cat(sprintf("  MARGINAL wicket cost (wicket minus dot, wicket-weighted): %+.2f runs\n", cost))
  cat(sprintf("  lambda in use for this format                          : %+.2f runs\n", -lam))
  cat(sprintf("  ratio |cost| / lambda                                  : %.2f\n", abs(cost)/lam))
  cat("\n  by innings:\n")
  for (inn in 1:2) {
    a2 <- d[innings_number==inn][, .(nw=sum(is_wicket==1), nd=sum(is_wicket==0),
             mw=mean(tsa[is_wicket==1]), md=mean(tsa[is_wicket==0])),
             by=.(over_number, wpre=pmin(wpre,9))][nw>=5 & nd>=20]
    if (nrow(a2)) cat(sprintf("    innings %d: %+.2f runs (%s wickets over %s states)\n",
        inn, a2[, sum((mw-md)*nw)/sum(nw)], format(a2[,sum(nw)],big.mark=","), nrow(a2)))
  }
  cat("\n  by phase (innings 1 only, cleanest -- no chase truncation):\n")
  a3 <- d[innings_number==1][, .(nw=sum(is_wicket==1), nd=sum(is_wicket==0),
           mw=mean(tsa[is_wicket==1]), md=mean(tsa[is_wicket==0])),
           by=.(over_number, wpre=pmin(wpre,9))][nw>=5 & nd>=20]
  mx <- if (fmt=="T20") 20 else 50
  a3[, phase := fifelse(over_number < mx*0.3, "early", fifelse(over_number < mx*0.8, "middle", "late"))]
  p <- a3[, .(wickets=sum(nw), cost=sum((mw-md)*nw)/sum(nw)), by=phase]
  for (i in 1:nrow(p)) cat(sprintf("    %-7s %+7.2f runs (%s wickets)\n",
      p$phase[i], p$cost[i], format(p$wickets[i], big.mark=",")))
}
