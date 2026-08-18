# Is the CSA factor era-dependent? Fit it at several as_at dates: a competition
# with stable difficulty should give a stable factor.
suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
conn <- DBI::dbConnect(duckdb::duckdb(),
  dbdir="C:/dev/bouncerverse/bouncerdata/bouncer.duckdb", read_only=TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown=TRUE), add=TRUE)
id_map <- build_player_id_map(conn)

watch <- c("CSA T20 Challenge","Vitality Blast","Indian Premier League",
           "Big Bash League","Caribbean Premier League","Bangladesh Premier League",
           "Super Smash","Pakistan Super League")
res <- data.table()
for (a in c("2016-01-01","2018-01-01","2020-01-01","2022-01-01","2024-01-01", NA)) {
  f <- tryCatch(suppressMessages(fit_competition_factors(conn,"t20","male",
        id_map=id_map, as_at=if (is.na(a)) NULL else as.Date(a))), error=function(e) NULL)
  if (is.null(f)) next
  f <- as.data.table(f)[comp %in% watch]
  f[, as_at := if (is.na(a)) "all data" else a]
  res <- rbind(res, f[, .(comp, as_at, factor = round(factor,3), n_bridges)], fill=TRUE)
}
w <- dcast(res, comp ~ as_at, value.var = "factor")
cat("\n=== competition factor by as_at date ===\n")
print(w)
cat("\n=== instability: max/min across the eras where each was rated ===\n")
st <- res[, .(eras = .N, lo = min(factor), hi = max(factor),
              swing = round(max(factor)/min(factor), 2)), by = comp]
setorder(st, -swing)
print(st)
cat("\n  A competition whose difficulty is stable should show swing ~1.0.\n")
cat("  The rating applies ONE factor to all of a competition's history, so a\n")
cat("  large swing means that single number is wrong in most eras.\n")
