suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
DB <- "C:/dev/bouncerverse/bouncerdata/bouncer.duckdb"
stopifnot(file.exists(DB), file.info(DB)$size > 1e10)
conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = DB, read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

cat("reference set:", default_competition_reference("test", "male"), "\n\n")

cat("=== bridge network: how many players appear in 2+ units? ===\n")
d <- as.data.table(DBI::dbGetQuery(conn, sprintf("
  SELECT d.batter_id, %s AS comp, COUNT(*) AS balls
  FROM cricsheet.deliveries d JOIN cricsheet.matches m ON m.match_id=d.match_id
  WHERE LOWER(d.match_type) IN ('test','mdm') AND m.gender='male'
    AND COALESCE(m.balls_per_over,6)=6 AND COALESCE(d.wides,0)=0
  GROUP BY d.batter_id, %s", .competition_sql("test"), .competition_sql("test"))))
cat(sprintf("  rows %d, unmapped balls %s\n", nrow(d),
            format(d[is.na(comp), sum(balls)], big.mark = ",")))
d <- d[!is.na(comp)]

cat("\n  balls per unit:\n")
print(d[, .(players = .N, balls = sum(balls)), by = comp][order(-balls)])

thr <- 30L
dd <- d[balls >= thr]
nb <- dd[, .(units = uniqueN(comp)), by = batter_id]
cat(sprintf("\n  players with >= %d balls in 2+ units: %d of %d (%.1f%%)\n",
            thr, nb[units >= 2, .N], nrow(nb), 100*nb[units>=2,.N]/nrow(nb)))
cat("\n  bridges TO the reference unit (Test) specifically:\n")
ref_players <- dd[comp == "Test", unique(batter_id)]
print(dd[comp != "Test" & batter_id %in% ref_players,
         .(bridge_players = uniqueN(batter_id)), by = comp][order(-bridge_players)])
cat("  a unit with < 3 bridges to Test cannot be placed directly (min_players=3)\n")

cat("\n=== fitting competition factors ===\n")
f <- fit_competition_factors(conn = conn, format = "test", gender = "male")
print(as.data.frame(f))

cat("\n=== PRE-DECLARED CHECK: domestic units should be EASIER than Test (factor > 1) ===\n")
fd <- as.data.table(f)
if ("competition" %in% names(fd)) setnames(fd, "competition", "comp")
cn <- intersect(c("comp", "unit", "event_name"), names(fd))[1]
vn <- intersect(c("factor", "difficulty", "strength"), names(fd))[1]
cat(sprintf("  (using columns %s / %s)\n", cn, vn))
for (i in seq_len(nrow(fd))) {
  u <- as.character(fd[[cn]][i]); v <- fd[[vn]][i]
  verdict <- if (u == "Test") "reference" else if (v > 1) "easier than Test  OK" else "*** HARDER than Test -- red flag ***"
  cat(sprintf("  %-28s %6.3f  %s\n", u, v, verdict))
}
