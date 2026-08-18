# Is the pooled career-mean advantage really "it predicts the LEAGUE"?
#
# Test: how well does a predictor that knows NOTHING about the player -- just
# the average raw output of the competition he is about to play in -- predict
# his next match? If that alone correlates substantially, then the career mean
# is getting credit for league prediction, and a rating that removes the league
# effect must lose that ground when pooled.
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

pm <- b[, .(v_adj = sum((raa - bo)/cf), v_opp = sum(raa - bo), raw = sum(raa),
            comp = comp[1]), by=.(player_id=batter_id, match_id, match_date)]
setorder(pm, player_id, match_date, match_id)
pm[, idx := seq_len(.N), by=player_id]
mk <- function(col) pm[, { cs<-cumsum(get(col)); c(NA, cs[-.N]/seq_len(.N-1L)) }, by=player_id]$V1
pm[, m_raw := mk("raw")][, m_opp := mk("v_opp")][, m_adj := mk("v_adj")]

# league-only predictor: the competition's mean raw output, computed from
# PRE-CUTOFF matches only, knowing nothing whatever about the player
lg <- pm[match_date < CUT, .(league_mean = mean(raw)), by=comp]
pm <- merge(pm, lg, by="comp", all.x=TRUE)
setorder(pm, player_id, match_date, match_id)

e <- pm[idx-1L >= 10L & match_date >= CUT & is.finite(m_raw) & is.finite(league_mean)]
sp <- function(a) cor(a, e$raw, method="spearman")
cat(sprintf("\nrows %s\n\n", format(nrow(e), big.mark=",")))
cat("=== POOLED: correlation with next-match RAW output ===\n")
cat(sprintf("  %-42s %+.4f\n", "LEAGUE MEAN ALONE (knows nothing of the player)", sp(e$league_mean)))
cat(sprintf("  %-42s %+.4f\n", "career mean of raw output", sp(e$m_raw)))
cat(sprintf("  %-42s %+.4f\n", "career mean, opponent-adjusted only", sp(e$m_opp)))
cat(sprintf("  %-42s %+.4f\n", "career mean, opponent + competition adj", sp(e$m_adj)))

cat("\n=== the same, WITHIN competition (n-weighted over comps with n>=300) ===\n")
w <- e[, .(n=.N,
           raw = cor(m_raw, raw, method="spearman"),
           opp = cor(m_opp, raw, method="spearman"),
           adj = cor(m_adj, raw, method="spearman")), by=comp][n>=300]
cat(sprintf("  %-42s %+.4f\n", "career mean of raw output", weighted.mean(w$raw, w$n)))
cat(sprintf("  %-42s %+.4f\n", "career mean, opponent-adjusted only", weighted.mean(w$opp, w$n)))
cat(sprintf("  %-42s %+.4f\n", "career mean, opponent + competition adj", weighted.mean(w$adj, w$n)))
cat("\n  If the league mean alone predicts, the pooled career mean is partly being\n")
cat("  rewarded for knowing the league -- exactly what the rating removes.\n")
