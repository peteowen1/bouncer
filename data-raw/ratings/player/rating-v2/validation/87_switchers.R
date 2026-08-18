# Do the DERIVED competition factors hold up out of sample?
#
# Design: fit factors using only data up to a cutoff (as_at, which truncates
# BEFORE fitting), then test them on deliveries AFTER the cutoff. Nothing in the
# test period touched the factors.
#
# The claim a factor makes is multiplicative: raw RAA in a competition equals the
# player's skill times that competition's factor. If true, dividing a player's
# per-competition mean by the factor should make their numbers AGREE across
# competitions. So the test is paired and within-player, which cancels skill:
# does adjustment SHRINK the spread of one player's per-competition means?
#
# PRE-DECLARED: adjustment must reduce mean within-player spread. If it does not,
# the factors are not carrying real competition strength.
setwd("C:/dev/bouncerverse/bouncer")
suppressMessages(devtools::load_all(".", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
conn <- DBI::dbConnect(duckdb::duckdb(),
  dbdir="C:/dev/bouncerverse/bouncerdata/bouncer.duckdb", read_only=TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown=TRUE), add=TRUE)

CUT <- "2024-01-01"; MINB <- 150L
cat(sprintf("cutoff %s | factors fitted on data <= cutoff | tested after it\n", CUT))
cat(sprintf("minimum %d balls per player per competition in the test window\n\n", MINB))

fac <- fit_competition_factors(conn, "t20", "male", as_at = CUT)
fmap <- setNames(fac$factor, fac$comp)
cat(sprintf("factors fitted: %d competitions\n", nrow(fac)))

b <- as.data.table(DBI::dbGetQuery(conn, sprintf("
  SELECT r.batter_id, r.match_date, r.raa, %s AS comp
  FROM main.cricsheet_ball_raa r JOIN cricsheet.matches m ON m.match_id=r.match_id
  WHERE r.format='T20' AND r.gender='male' AND r.match_date > DATE '%s'",
  bouncer:::.competition_sql("t20"), CUT)))
idmap <- build_player_id_map(conn); canonicalise_player_ids(b, idmap)
b[, cf := fmap[comp]]
cat(sprintf("test-window deliveries: %s  (%.1f%% have a fitted factor)\n",
    format(nrow(b), big.mark=","), 100*mean(!is.na(b$cf))))
b <- b[!is.na(cf)]

pc <- b[, .(balls=.N, raw=mean(raa)), by=.(batter_id, comp)][balls >= MINB]
pc[, adj := raw / fmap[comp]]
sw <- pc[, .N, by=batter_id][N >= 2, batter_id]
pc <- pc[batter_id %in% sw]
cat(sprintf("\nswitchers in the test window: %s players, %s player-competition cells\n",
    format(length(sw), big.mark=","), format(nrow(pc), big.mark=",")))
if (length(sw) < 30) { cat("too few switchers to conclude anything\n"); quit(save="no") }

sp <- pc[, .(n=.N,
             spread_raw = max(raw) - min(raw),
             spread_adj = max(adj) - min(adj),
             sd_raw = sd(raw), sd_adj = sd(adj)), by=batter_id]
cat("\n=== within-player spread across competitions (runs per ball) ===\n")
cat(sprintf("  mean range  raw %.4f -> adjusted %.4f   change %+.1f%%\n",
    mean(sp$spread_raw), mean(sp$spread_adj),
    100*(mean(sp$spread_adj)-mean(sp$spread_raw))/mean(sp$spread_raw)))
cat(sprintf("  mean sd     raw %.4f -> adjusted %.4f   change %+.1f%%\n",
    mean(sp$sd_raw, na.rm=TRUE), mean(sp$sd_adj, na.rm=TRUE),
    100*(mean(sp$sd_adj,na.rm=TRUE)-mean(sp$sd_raw,na.rm=TRUE))/mean(sp$sd_raw,na.rm=TRUE)))
cat(sprintf("  players improved by adjustment: %d of %d (%.1f%%)\n",
    sp[spread_adj < spread_raw, .N], nrow(sp), 100*sp[spread_adj<spread_raw,.N]/nrow(sp)))

set.seed(42)
d <- sp$spread_raw - sp$spread_adj
bs <- replicate(2000, mean(sample(d, length(d), replace=TRUE)))
ci <- quantile(bs, c(.025,.975))
cat(sprintf("\n  reduction in spread: %+.4f runs/ball, 95%% CI [%+.4f, %+.4f]  -> %s\n",
    mean(d), ci[1], ci[2],
    ifelse(ci[1] > 0, "FACTORS HELP (CI excludes zero)",
    ifelse(ci[2] < 0, "FACTORS HURT", "NOT DISTINGUISHABLE FROM ZERO"))))

cat("\n=== placebo: shuffle the factors across competitions ===\n")
set.seed(7); shuf <- fmap; names(shuf) <- sample(names(fmap))
pc[, adj_p := raw / shuf[comp]]
sp2 <- pc[, .(spread_p = max(adj_p) - min(adj_p)), by=batter_id]
cat(sprintf("  mean range with SHUFFLED factors %.4f (real %.4f, raw %.4f)\n",
    mean(sp2$spread_p), mean(sp$spread_adj), mean(sp$spread_raw)))
