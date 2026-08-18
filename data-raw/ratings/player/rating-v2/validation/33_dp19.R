# Re-derive D-P19's +40.5% for the opponent adjustment, against BOTH possible
# targets, to find out whether the recorded gain is real or circular.
#
# D-P19 records: "raw career mean 0.1699 -> opponent-adjusted 0.1771 (+4.3%)"
# and "ITERATED: 0.1711 raw -> 0.2404 (20 iters), +40.5%", through the #38
# harness (next-10-match average, Spearman), min 10 prior matches.
#
# The question it does not record is what the TARGET was. If the target is the
# forward ADJUSTED value, an adjusted predictor wins by construction -- it is
# predicting its own transformation. If the target is forward RAW output, the
# gain is real. Both are computed here from identical inputs.
suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
DB <- "C:/dev/bouncerverse/bouncerdata/bouncer.duckdb"
stopifnot(file.exists(DB), file.info(DB)$size > 1e10)
conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = DB, read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
id_map <- build_player_id_map(conn)

# D-P19 used the composite `raa` and the full T20 male pool.
b <- as.data.table(DBI::dbGetQuery(conn, "
  SELECT match_id, match_date, batter_id, bowler_id, raa
  FROM main.cricsheet_ball_raa WHERE format='T20' AND gender='male'"))
canonicalise_player_ids(b, id_map)
cat(sprintf("deliveries: %s\n", format(nrow(b), big.mark=",")))

# Adjustment fitted on ALL data, exactly as D-P19's iterated two-way fit was.
eff <- fit_two_way_effects(b, prior_balls = 60, iterations = 20)
b[eff$bowler, on = "bowler_id", bo := i.eff][is.na(bo), bo := 0]
b[, adj := raa - bo]

pm <- b[, .(v_raw = sum(raa), v_adj = sum(adj)),
        by = .(player_id = batter_id, match_id, match_date)]
setorder(pm, player_id, match_date, match_id)
pm[, idx := seq_len(.N), by = player_id]
cat(sprintf("player-matches: %s, players: %d\n",
            format(nrow(pm), big.mark=","), uniqueN(pm$player_id)))

# Strictly-prior career means of each
pm[, m_raw := { cs <- cumsum(v_raw); c(NA, cs[-.N]/seq_len(.N-1L)) }, by = player_id]
pm[, m_adj := { cs <- cumsum(v_adj); c(NA, cs[-.N]/seq_len(.N-1L)) }, by = player_id]

H <- 10L
fw <- function(col) pm[, { cs <- cumsum(get(col)); k <- pmin(.N, idx+H-1L)
                           (cs[k]-c(0,cs)[idx])/(k-idx+1L) }, by = player_id]$V1
pm[, f_raw := fw("v_raw")]
pm[, f_adj := fw("v_adj")]

e <- pm[idx-1L >= 10L & is.finite(m_raw) & is.finite(m_adj) &
          is.finite(f_raw) & is.finite(f_adj)]
cat(sprintf("evaluated rows: %s\n\n", format(nrow(e), big.mark=",")))

sp <- function(a, b) cor(a, b, method = "spearman")
cat("=== next-10-match average, Spearman ===\n")
cat(sprintf("  %-34s %8s %8s %9s\n", "target", "raw pred", "adj pred", "gain"))
r1 <- sp(e$m_raw, e$f_raw); a1 <- sp(e$m_adj, e$f_raw)
r2 <- sp(e$m_raw, e$f_adj); a2 <- sp(e$m_adj, e$f_adj)
cat(sprintf("  %-34s %8.4f %8.4f %+8.1f%%\n", "forward RAW output", r1, a1, 100*(a1-r1)/abs(r1)))
cat(sprintf("  %-34s %8.4f %8.4f %+8.1f%%\n", "forward ADJUSTED value", r2, a2, 100*(a2-r2)/abs(r2)))

cat("\n  D-P19 recorded raw 0.1711 -> adjusted 0.2404 (+40.5%).\n")
cat("  Whichever row reproduces that pair identifies the target it used.\n")
