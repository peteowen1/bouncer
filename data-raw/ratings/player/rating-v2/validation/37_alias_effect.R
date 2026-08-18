# What does merging the sponsor variants actually buy?
suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
conn <- DBI::dbConnect(duckdb::duckdb(),
  dbdir="C:/dev/bouncerverse/bouncerdata/bouncer.duckdb", read_only=TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown=TRUE), add=TRUE)
id_map <- build_player_id_map(conn)

cat("=== alias_competition() sanity ===\n")
print(data.table(raw = c("NatWest T20 Blast","Vitality Blast Men","Indian Premier League",
                         "Royal London One-Day Cup","Rachael Heyhoe Flint Trophy"),
                 canonical = alias_competition(c("NatWest T20 Blast","Vitality Blast Men",
                   "Indian Premier League","Royal London One-Day Cup",
                   "Rachael Heyhoe Flint Trophy"))))

for (bk in list(c("t20","male"), c("odi","male"), c("odi","female"), c("t20","female"))) {
  f <- bk[1]; g <- bk[2]
  fac <- tryCatch(suppressMessages(fit_competition_factors(conn, f, g, id_map=id_map)),
                  error=function(e) { cat(sprintf("\n%s %s: FAILED -- %s\n", f, g, conditionMessage(e))); NULL })
  if (is.null(fac)) next
  fac <- as.data.table(fac)
  b <- as.data.table(DBI::dbGetQuery(conn, sprintf("
    SELECT COALESCE(%s,'unknown') AS comp, COUNT(*) AS balls
    FROM main.cricsheet_ball_raa r JOIN cricsheet.matches m ON m.match_id=r.match_id
    WHERE r.format='%s' AND r.gender='%s' GROUP BY 1", .competition_sql(f), toupper(f), g)))
  b[, rated := comp %in% fac$comp]
  cat(sprintf("\n%s %s: %d comps rated (%d direct, %d chained), unrated %.2f%% of balls\n",
      f, g, nrow(fac), sum(fac$step==0, na.rm=TRUE), sum(fac$step>0, na.rm=TRUE),
      100*b[rated==FALSE, sum(balls)]/b[, sum(balls)]))
  key <- c("Vitality Blast","CSA T20 Challenge","ICC Men's T20 World Cup",
           "One-Day Cup","ECB Women's One-Day Cup","ICC Cricket World Cup")
  k <- fac[comp %in% key]
  if (nrow(k)) { setorder(k, -n_bridges)
    for (i in 1:nrow(k)) cat(sprintf("    %-28s factor %.3f  bridges %s\n",
      k$comp[i], k$factor[i], ifelse(is.na(k$n_bridges[i]),"reference",k$n_bridges[i]))) }
}
