# Was "+15.9% within competition" an artefact of averaging PERCENTAGE gains?
#
# A percentage gain has the baseline correlation in its denominator. A
# competition whose baseline is near zero turns a tiny absolute change into a
# huge percentage, and an n-weighted mean of percentages is then dominated by
# the noisiest cells. The correct comparison aggregates the CORRELATIONS.
suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
conn <- DBI::dbConnect(duckdb::duckdb(),
  dbdir="C:/dev/bouncerverse/bouncerdata/bouncer.duckdb", read_only=TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown=TRUE), add=TRUE)
CUT <- as.Date("2018-01-01"); id_map <- build_player_id_map(conn)
b <- as.data.table(DBI::dbGetQuery(conn, sprintf("
  SELECT r.match_id, r.match_date, r.batter_id, r.bowler_id, r.raa,
         COALESCE(%s,'unknown') AS comp
  FROM main.cricsheet_ball_raa r JOIN cricsheet.matches m ON m.match_id=r.match_id
  WHERE r.format='T20' AND r.gender='male'", .competition_sql("t20"))))
canonicalise_player_ids(b, id_map)
fac <- as.data.table(fit_competition_factors(conn,"t20","male",id_map=id_map,as_at=CUT-1L))
fmap <- setNames(fac$factor, fac$comp)
eff <- fit_two_way_effects(b[match_date < CUT], prior_balls=60, iterations=20)
b[, cf := fmap[comp]][is.na(cf), cf := 1]
b[eff$bowler, on="bowler_id", bo := i.eff][is.na(bo), bo := 0]
pm <- b[, .(v=sum((raa-bo)/cf), raw=sum(raa), comp=comp[1]),
        by=.(player_id=batter_id, match_id, match_date)]
setorder(pm, player_id, match_date, match_id); pm[, idx := seq_len(.N), by=player_id]
pop <- pm[, mean(v)]
dec <- function(v,dt,d,pr,pp){n<-length(v);rt<-rep(NA_real_,n);sw<-0;sv<-0
  if(n>=2L) for(i in 2:n){a<-exp(-as.numeric(dt[i]-dt[i-1L])/d)
    sv<-a*(sv+v[i-1L]);sw<-a*(sw+1);rt[i]<-(sv+pr*pp)/(sw+pr)};rt}
pm[, rt := dec(v, match_date, 1095, 20, pop), by=player_id]
pm[, cw := { cs<-cumsum(raw); c(NA, cs[-.N]/seq_len(.N-1L)) }, by=player_id]
e <- pm[idx-1L >= 10L & match_date >= CUT & is.finite(rt) & is.finite(cw)]

k <- e[, .(n=.N, r=cor(rt,raw,method="spearman"), c=cor(cw,raw,method="spearman")),
       by=comp][n>=300]
k[, pct := 100*(r-c)/abs(c)]
cat(sprintf("\n%d competitions with n>=300, %s rows\n\n", nrow(k),
            format(sum(k$n), big.mark=",")))
cat("  === the two ways of aggregating ===\n")
cat(sprintf("  n-weighted mean of per-competition PERCENT gains : %+.1f%%   <- what I reported\n",
            weighted.mean(k$pct, k$n)))
cat(sprintf("  n-weighted correlations, then compared          : %+.1f%%   <- correct\n",
            100*(weighted.mean(k$r,k$n)-weighted.mean(k$c,k$n))/abs(weighted.mean(k$c,k$n))))
cat("\n  === why they differ: small baselines blow up the ratio ===\n")
setorder(k, c)
cat(sprintf("  %-30s %7s %9s %9s %9s\n","competition","n","baseline","rating","pct gain"))
for (i in 1:min(6,nrow(k))) cat(sprintf("  %-30s %7s %9.4f %9.4f %+8.1f%%\n",
  substr(k$comp[i],1,30), format(k$n[i],big.mark=","), k$c[i], k$r[i], k$pct[i]))
cat(sprintf("\n  correlation between |baseline| and percent gain: %+.2f\n",
            cor(abs(k$c), k$pct, method="spearman")))
cat("  strongly negative => the biggest percentage gains are exactly the\n")
cat("  competitions with the smallest, noisiest baselines.\n")
