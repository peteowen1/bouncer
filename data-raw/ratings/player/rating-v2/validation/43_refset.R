# Does a SMALLER reference set improve the competition adjustment?
#
# The reference set pins every competition in it to exactly 1.0, so with 8
# competitions covering 47% of T20 male balls the model cannot say the IPL is
# harder than the county Blast. Narrowing it should let those be estimated.
#
# PRE-DECLARED: a narrower reference should raise the within-competition and
# switcher gains, and widen the factor spread. FALSIFIER: if a single-competition
# anchor makes them WORSE -- thin bridges giving noisy factors, more chaining,
# more unrated balls -- the current broad set is vindicated and stays.
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
eff <- fit_two_way_effects(b[match_date < CUT], prior_balls=60, iterations=20)
b[eff$bowler, on="bowler_id", bo := i.eff][is.na(bo), bo := 0]
tot <- b[, .N]

variants <- list(
  "current (8)"      = COMPETITION_REFERENCE_T20,
  "elite 4"          = c("Indian Premier League","Big Bash League",
                         "Pakistan Super League","SA20"),
  "IPL + BBL"        = c("Indian Premier League","Big Bash League"),
  "IPL only"         = "Indian Premier League")

dec <- function(v, dt, d, prior, pp) { n<-length(v); rt<-rep(NA_real_,n); sw<-0; sv<-0
  if (n>=2L) for (i in 2:n) { a<-exp(-as.numeric(dt[i]-dt[i-1L])/d)
    sv<-a*(sv+v[i-1L]); sw<-a*(sw+1); rt[i]<-(sv+prior*pp)/(sw+prior) }; rt }
gain <- function(d) if (nrow(d) < 300) NA_real_ else
  100*(cor(d$rt,d$f,method="spearman") - cor(d$cw,d$f,method="spearman")) /
      abs(cor(d$cw,d$f,method="spearman"))

cat(sprintf("\n  %-14s %7s %7s %8s %9s %9s %9s %9s\n","reference","comps",
            "pinned","median f","chained","pooled","within","switchers"))
cat("  ", strrep("-", 84), "\n", sep="")
for (nm in names(variants)) {
  fac <- tryCatch(suppressMessages(fit_competition_factors(conn,"t20","male",
          reference=variants[[nm]], id_map=id_map, as_at=CUT-1L)), error=function(e) NULL)
  if (is.null(fac)) { cat(sprintf("  %-14s FAILED\n", nm)); next }
  fac <- as.data.table(fac)
  fmap <- setNames(fac$factor, fac$comp)
  pinned <- 100 * b[comp %in% variants[[nm]], .N] / tot
  est <- fac[!comp %in% variants[[nm]]]

  d <- copy(b); d[, cf := fmap[comp]][is.na(cf), cf := 1]
  d[, val := (raa - bo)/cf]
  pm <- d[, .(v=sum(val), raw=sum(raa), comp=comp[1]),
          by=.(player_id=batter_id, match_id, match_date)]
  setorder(pm, player_id, match_date, match_id)
  pm[, idx := seq_len(.N), by=player_id]
  pop <- pm[, mean(v)]
  pm[, rt := dec(v, match_date, 1095, 20, pop), by=player_id]
  pm[, cw := { cs<-cumsum(raw); c(NA, cs[-.N]/seq_len(.N-1L)) }, by=player_id]
  pm[, f := raw]
  pm[, sw := { pc <- shift(comp); !is.na(pc) & pc != comp }, by=player_id]
  e <- pm[idx-1L >= MIN_PRIOR & match_date >= CUT & is.finite(rt) & is.finite(cw)]
  # within-competition: n-weighted mean of the per-competition gain
  wg <- e[, .(n=.N, g=gain(.SD)), by=comp][n>=300 & is.finite(g)]
  cat(sprintf("  %-14s %7d %6.1f%% %8.3f %9d %+8.1f%% %+8.1f%% %+8.1f%%\n",
      nm, nrow(fac), pinned, median(est$factor), sum(fac$step>0, na.rm=TRUE),
      gain(e), weighted.mean(wg$g, wg$n), gain(e[sw==TRUE])))
}
cat("\n  pooled = all player-matches; within = n-weighted over competitions with\n")
cat("  n>=300; switchers = player-matches following a different competition.\n")
