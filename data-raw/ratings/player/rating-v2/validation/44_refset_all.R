# Pick each bucket's reference set on evidence, using the within-competition and
# switcher gains -- the two metrics that can actually see a competition
# adjustment (D-P36). Pooled is reported but must not drive the choice.
suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
conn <- DBI::dbConnect(duckdb::duckdb(),
  dbdir="C:/dev/bouncerverse/bouncerdata/bouncer.duckdb", read_only=TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown=TRUE), add=TRUE)
CUT <- as.Date("2018-01-01"); MIN_PRIOR <- 10L
id_map <- build_player_id_map(conn)

dec <- function(v, dt, d, prior, pp) { n<-length(v); rt<-rep(NA_real_,n); sw<-0; sv<-0
  if (n>=2L) for (i in 2:n) { a<-exp(-as.numeric(dt[i]-dt[i-1L])/d)
    sv<-a*(sv+v[i-1L]); sw<-a*(sw+1); rt[i]<-(sv+prior*pp)/(sw+prior) }; rt }
gain <- function(d) if (nrow(d) < 300) NA_real_ else
  100*(cor(d$rt,d$f,method="spearman") - cor(d$cw,d$f,method="spearman")) /
      abs(cor(d$cw,d$f,method="spearman"))

BUCKETS <- list(
  list(f="odi", g="male", v=list(
    "current (4)" = COMPETITION_REFERENCE_ODI,
    "ICC events"  = c("ICC Cricket World Cup","ICC Champions Trophy",
                      "ICC Men's Cricket World Cup Super League"),
    "World Cup"   = "ICC Cricket World Cup")),
  list(f="t20", g="female", v=list(
    "current (7)" = COMPETITION_REFERENCE_T20_FEMALE,
    "elite 3"     = c("Women's Big Bash League","Women's Premier League",
                      "ICC Women's T20 World Cup"),
    "WBBL + WPL"  = c("Women's Big Bash League","Women's Premier League"))),
  list(f="odi", g="female", v=list(
    "current (5)" = COMPETITION_REFERENCE_ODI_FEMALE,
    "intl only"   = c("ICC Women's World Cup","ICC Women's Championship",
                      "Women's Ashes","ICC Women's Cricket World Cup"),
    "WC + Champ"  = c("ICC Women's World Cup","ICC Women's Championship"))))

for (bk in BUCKETS) {
  f <- bk$f; g <- bk$g
  b <- as.data.table(DBI::dbGetQuery(conn, sprintf("
    SELECT r.match_id, r.match_date, r.batter_id, r.bowler_id, r.raa,
           COALESCE(%s,'unknown') AS comp
    FROM main.cricsheet_ball_raa r JOIN cricsheet.matches m ON m.match_id=r.match_id
    WHERE r.format='%s' AND r.gender='%s'", .competition_sql(f), toupper(f), g)))
  canonicalise_player_ids(b, id_map)
  eff <- fit_two_way_effects(b[match_date < CUT], prior_balls=60, iterations=20)
  b[eff$bowler, on="bowler_id", bo := i.eff][is.na(bo), bo := 0]
  tot <- b[, .N]
  cat(sprintf("\n=== %s %s ===\n", toupper(f), g))
  cat(sprintf("  %-14s %7s %8s %9s %9s %9s\n","reference","pinned","chained",
              "pooled","within","switchers"))
  for (nm in names(bk$v)) {
    fac <- tryCatch(suppressMessages(fit_competition_factors(conn,f,g,
            reference=bk$v[[nm]], id_map=id_map, as_at=CUT-1L)), error=function(e) NULL)
    if (is.null(fac)) { cat(sprintf("  %-14s FAILED\n", nm)); next }
    fac <- as.data.table(fac); fmap <- setNames(fac$factor, fac$comp)
    d <- copy(b); d[, cf := fmap[comp]][is.na(cf), cf := 1]
    d[, val := (raa - bo)/cf]
    pm <- d[, .(v=sum(val), raw=sum(raa), comp=comp[1]),
            by=.(player_id=batter_id, match_id, match_date)]
    setorder(pm, player_id, match_date, match_id)
    pm[, idx := seq_len(.N), by=player_id]; pop <- pm[, mean(v)]
    pm[, rt := dec(v, match_date, 1095, 20, pop), by=player_id]
    pm[, cw := { cs<-cumsum(raw); c(NA, cs[-.N]/seq_len(.N-1L)) }, by=player_id]
    pm[, f := raw]
    pm[, sw := { pc <- shift(comp); !is.na(pc) & pc != comp }, by=player_id]
    e <- pm[idx-1L >= MIN_PRIOR & match_date >= CUT & is.finite(rt) & is.finite(cw)]
    wg <- e[, .(n=.N, gg=gain(.SD)), by=comp][n>=300 & is.finite(gg)]
    cat(sprintf("  %-14s %6.1f%% %8d %+8.1f%% %+8.1f%% %+8.1f%%\n", nm,
        100*b[comp %in% bk$v[[nm]], .N]/tot, sum(fac$step>0, na.rm=TRUE),
        gain(e), if (nrow(wg)) weighted.mean(wg$gg, wg$n) else NA_real_,
        gain(e[sw==TRUE])))
    cat(sprintf("                 n=%s  switchers=%s  comps scored=%d
",
        format(nrow(e), big.mark=","), format(e[sw==TRUE,.N], big.mark=","), nrow(wg)))
  }
}
