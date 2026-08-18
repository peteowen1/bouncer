# How much of each bucket sits in a competition PINNED to 1.0 by the reference
# set? Anything pinned gets no competition adjustment at all, by construction.
suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
conn <- DBI::dbConnect(duckdb::duckdb(),
  dbdir="C:/dev/bouncerverse/bouncerdata/bouncer.duckdb", read_only=TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown=TRUE), add=TRUE)
id_map <- build_player_id_map(conn)

for (bk in list(c("t20","male"),c("odi","male"),c("t20","female"),c("odi","female"))) {
  f <- bk[1]; g <- bk[2]
  ref <- default_competition_reference(f, g)
  fac <- as.data.table(suppressMessages(fit_competition_factors(conn,f,g,id_map=id_map)))
  b <- as.data.table(DBI::dbGetQuery(conn, sprintf("
    SELECT COALESCE(%s,'unknown') AS comp, COUNT(*) AS balls
    FROM main.cricsheet_ball_raa r JOIN cricsheet.matches m ON m.match_id=r.match_id
    WHERE r.format='%s' AND r.gender='%s' GROUP BY 1", .competition_sql(f), toupper(f), g)))
  tot <- b[, sum(balls)]
  pin <- b[comp %in% ref, sum(balls)]
  fac2 <- fac[!comp %in% ref]
  cat(sprintf("\n=== %s %s ===\n", f, g))
  cat(sprintf("  reference set: %d competitions -- %s\n", length(ref),
              paste(substr(ref,1,26), collapse=" | ")))
  cat(sprintf("  balls PINNED at factor 1.0 (no adjustment possible): %s of %s = %.1f%%\n",
              format(pin, big.mark=","), format(tot, big.mark=","), 100*pin/tot))
  if (nrow(fac2)) cat(sprintf("  estimated factors: n=%d, median %.3f, IQR [%.3f, %.3f], range [%.2f, %.2f]\n",
    nrow(fac2), median(fac2$factor), quantile(fac2$factor,.25), quantile(fac2$factor,.75),
    min(fac2$factor), max(fac2$factor)))
  cat(sprintf("  estimated factors within 5%% of 1.0 (effectively no discount): %.1f%%\n",
              100*mean(abs(fac2$factor-1) < 0.05)))
}
