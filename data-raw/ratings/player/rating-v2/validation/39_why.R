# WHY does nothing beat a career mean at next-1? Four questions:
#   1. what is the sample size, overall and per competition?
#   2. what is the rating-vs-career gap WITHIN each competition?
#   3. how many player-matches are actually a COMPETITION SWITCH?
#   4. what is the gap for switchers specifically -- the only players for whom
#      a competition adjustment can possibly do anything?
#
# Hypothesis: the adjustment is diluted to nothing by pooling, because most
# player-matches are inside a single league where it has no work to do.
suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
conn <- DBI::dbConnect(duckdb::duckdb(),
  dbdir="C:/dev/bouncerverse/bouncerdata/bouncer.duckdb", read_only=TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown=TRUE), add=TRUE)
CUT <- as.Date("2018-01-01"); MIN_PRIOR <- 10L
id_map <- build_player_id_map(conn)

b <- as.data.table(DBI::dbGetQuery(conn, sprintf("
  SELECT r.match_id, r.match_date, r.batter_id, r.bowler_id, r.raa,
         COALESCE(%s,'unknown') AS comp
  FROM main.cricsheet_ball_raa r JOIN cricsheet.matches m ON m.match_id=r.match_id
  WHERE r.format='T20' AND r.gender='male'", .competition_sql("t20"))))
canonicalise_player_ids(b, id_map)
fac <- fit_competition_factors(conn,"t20","male",id_map=id_map,as_at=CUT-1L)
fmap <- setNames(fac$factor, fac$comp)
eff <- fit_two_way_effects(b[match_date < CUT], prior_balls=60, iterations=20)
b[, cf := fmap[comp]][is.na(cf), cf := 1]
b[eff$bowler, on="bowler_id", bo := i.eff][is.na(bo), bo := 0]
b[, val := (raa - bo)/cf]

pm <- b[, .(v = sum(val), raw = sum(raa), comp = comp[1]),
        by=.(player_id=batter_id, match_id, match_date)]
setorder(pm, player_id, match_date, match_id)
pm[, idx := seq_len(.N), by=player_id]
pop <- pm[, mean(v)]
dec <- function(v, dt, d, prior, pp) { n<-length(v); rt<-rep(NA_real_,n); sw<-0; sv<-0
  if (n>=2L) for (i in 2:n) { a<-exp(-as.numeric(dt[i]-dt[i-1L])/d)
    sv<-a*(sv+v[i-1L]); sw<-a*(sw+1); rt[i]<-(sv+prior*pp)/(sw+prior) }; rt }
pm[, rt := dec(v, match_date, 1095, 20, pop), by=player_id]
pm[, cw := { cs<-cumsum(raw); c(NA, cs[-.N]/seq_len(.N-1L)) }, by=player_id]
pm[, f  := raw]                                   # next-1 target IS this match
pm[, prev_comp := shift(comp), by=player_id]
pm[, switch := !is.na(prev_comp) & prev_comp != comp]
# how many DISTINCT competitions has he played before this match?
pm[, n_prior_comps := sapply(seq_len(.N), function(i)
     if (i<2) 0L else uniqueN(comp[1:(i-1L)])), by=player_id]

e <- pm[idx-1L >= MIN_PRIOR & match_date >= CUT & is.finite(rt) & is.finite(cw)]
sp <- function(d) if (nrow(d) < 200) NA_real_ else
  100*(cor(d$rt,d$f,method="spearman") - cor(d$cw,d$f,method="spearman")) /
      abs(cor(d$cw,d$f,method="spearman"))

cat(sprintf("\n=== 1. SAMPLE SIZE ===\n  player-matches scored: %s over %d players\n",
            format(nrow(e), big.mark=","), uniqueN(e$player_id)))
cat(sprintf("  overall gain, rating vs career mean: %+.1f%%\n", sp(e)))

cat("\n=== 2 & 3. BY COMPETITION (n>=300) ===\n")
cat(sprintf("  %-30s %7s %9s %9s %8s\n","competition","n","rating","career","gain"))
agg <- e[, .(n=.N, r=cor(rt,f,method="spearman"), c=cor(cw,f,method="spearman")),
         by=comp][n>=300]
setorder(agg, -n)
for (i in 1:nrow(agg)) cat(sprintf("  %-30s %7s %9.4f %9.4f %+7.1f%%\n",
  substr(agg$comp[i],1,30), format(agg$n[i], big.mark=","), agg$r[i], agg$c[i],
  100*(agg$r[i]-agg$c[i])/abs(agg$c[i])))

cat("\n=== 4. COMPETITION SWITCHERS -- where the adjustment can do work ===\n")
cat(sprintf("  player-matches that FOLLOW a different competition: %s of %s (%.1f%%)\n",
    format(e[switch==TRUE,.N], big.mark=","), format(nrow(e), big.mark=","),
    100*e[switch==TRUE,.N]/nrow(e)))
cat(sprintf("  %-34s %8s %8s\n","subset","n","gain"))
cat(sprintf("  %-34s %8s %+7.1f%%\n","same competition as previous match",
    format(e[switch==FALSE,.N], big.mark=","), sp(e[switch==FALSE])))
cat(sprintf("  %-34s %8s %+7.1f%%\n","SWITCHED competition",
    format(e[switch==TRUE,.N], big.mark=","), sp(e[switch==TRUE])))
for (k in 1:4) {
  d <- e[n_prior_comps == k]
  if (nrow(d) >= 300) cat(sprintf("  %-34s %8s %+7.1f%%\n",
    sprintf("played in %d prior competition%s", k, if (k>1) "s" else ""),
    format(nrow(d), big.mark=","), sp(d)))
}
d <- e[n_prior_comps >= 5]
if (nrow(d) >= 300) cat(sprintf("  %-34s %8s %+7.1f%%\n","played in 5+ prior competitions",
  format(nrow(d), big.mark=","), sp(d)))
