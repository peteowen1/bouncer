# Old versus new 3-way ELO, per player (bouncerverse#63).
#
# Reads main.{table}_prev_summary, written by
# validation/snapshot_3way_elo_ratings.R just before the rebuild promoted.
#
# ONLY meaningful where the constants did not change. The men's tables are
# like-for-like. The WOMEN'S are not: every women's table built before
# 2026-08-20 used men's K-factors, because the deleted THREE_WAY_*_T20
# constants were aliases to the MENS values. A women's diff mixes seven months
# of new matches with a corrected engine and cannot separate them.
#
# Usage: Rscript data-raw/validation/three_way_elo_old_vs_new.R mens t20
suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})

a <- commandArgs(trailingOnly = TRUE)
gender <- if (length(a) >= 1) a[1] else "mens"
fmt    <- if (length(a) >= 2) a[2] else "t20"
tbl  <- paste0(gender, "_", fmt, "_3way_elo")
snap <- paste0(tbl, "_prev_summary")
MIN_BALLS <- 200L

conn <- get_db_connection(read_only = TRUE)
on.exit(dbDisconnect(conn, shutdown = TRUE), add = TRUE)
if (!table_exists(conn, snap)) cli::cli_abort("No snapshot {.val {snap}}.")

if (gender == "womens") {
  cli::cli_alert_warning(c(
    "Women's tables are NOT like-for-like: the old one used men's K-factors."))
}

new <- as.data.table(dbGetQuery(conn, sprintf("
  WITH bat AS (
    SELECT batter_id AS player_id, batter_run_elo_after AS run_elo,
           COUNT(*) OVER (PARTITION BY batter_id) AS balls,
           ROW_NUMBER() OVER (PARTITION BY batter_id ORDER BY match_date DESC, delivery_id DESC) AS rn
    FROM main.%s)
  SELECT player_id, run_elo, balls FROM bat WHERE rn = 1", tbl)))
old <- as.data.table(dbGetQuery(conn, sprintf(
  "SELECT player_id, run_elo, balls FROM %s WHERE role = 'batter'", snap)))

m <- merge(old, new, by = "player_id", suffixes = c("_old", "_new"))
m <- m[balls_old >= MIN_BALLS & balls_new >= MIN_BALLS]

cli::cli_h1("{tbl}: old vs new")
cli::cli_alert_info("{format(nrow(old), big.mark=',')} old, {format(nrow(new), big.mark=',')} new, {format(nrow(m), big.mark=',')} comparable at {MIN_BALLS}+ balls both sides")
cli::cli_alert_info("new players (not in old): {format(nrow(new[!old, on='player_id']), big.mark=',')}")
cli::cli_alert_info("Spearman {round(cor(m$run_elo_old, m$run_elo_new, method='spearman'), 4)}; Pearson {round(cor(m$run_elo_old, m$run_elo_new), 4)}")
cli::cli_alert_info("mean {round(mean(m$run_elo_old),1)} -> {round(mean(m$run_elo_new),1)}; sd {round(sd(m$run_elo_old),1)} -> {round(sd(m$run_elo_new),1)}")

m[, `:=`(rank_old = frank(-run_elo_old), rank_new = frank(-run_elo_new))]
m[, move := rank_old - rank_new]
cli::cli_alert_info("median |rank move| {round(median(abs(m$move)),1)} of {nrow(m)}")

# Names AND country. A bare list of surnames is not checkable by eye -- the
# standing rule for any rating table here.
nm <- function(ids) {
  q <- dbGetQuery(conn, sprintf(
    "SELECT player_id, ANY_VALUE(player_name) AS nm, ANY_VALUE(country) AS ctry
     FROM cricsheet.players WHERE player_id IN (%s) GROUP BY player_id",
    paste(sprintf("'%s'", ids), collapse = ",")))
  setNames(ifelse(is.na(q$ctry) | q$ctry == "",
                  q$nm, paste0(q$nm, " (", q$ctry, ")")), q$player_id)
}
show <- function(d, title) {
  n <- nm(d$player_id)
  cat(sprintf("\n%s\n", title))
  for (i in seq_len(nrow(d))) {
    cat(sprintf("  %-22s %6.0f -> %6.0f   rank %4.0f -> %4.0f (%+.0f), %s balls\n",
        substr(n[[d$player_id[i]]] %||% d$player_id[i], 1, 22),
        d$run_elo_old[i], d$run_elo_new[i], d$rank_old[i], d$rank_new[i],
        d$move[i], format(d$balls_new[i], big.mark = ",")))
  }
}
cat(sprintf("
Format: %s %s, batter RUN ELO, %d+ balls. Start rating %.0f.
",
            toupper(gender), toupper(fmt), MIN_BALLS, THREE_WAY_ELO_START))
show(head(m[order(rank_new)], 10), "Top 10 now")
show(head(m[order(-move)], 5), "Biggest risers")
show(head(m[order(move)], 5), "Biggest fallers")
