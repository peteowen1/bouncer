# Whose perspective is unified_margin's SIGN? Documented as team1; measured?
suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
conn <- get_db_connection(read_only = TRUE); on.exit(dbDisconnect(conn, shutdown=TRUE), add=TRUE)
d <- as.data.table(dbGetQuery(conn, "
  SELECT m.match_id, m.team1, m.team2, m.outcome_winner, m.unified_margin,
         (SELECT batting_team FROM cricsheet.match_innings i
          WHERE i.match_id = m.match_id AND i.innings = 1 LIMIT 1) AS bat_first
  FROM cricsheet.matches m
  WHERE m.unified_margin IS NOT NULL AND m.outcome_winner IS NOT NULL
    AND LOWER(m.match_type) IN ('t20','it20','odi','odm')"))
d <- d[!is.na(bat_first) & unified_margin != 0]
d[, `:=`(team1_won = outcome_winner == team1,
         batfirst_won = outcome_winner == bat_first,
         pos = unified_margin > 0)]
cat(sprintf("matches with a decided result and a non-zero margin: %s\n\n",
            format(nrow(d), big.mark=",")))
cat("HYPOTHESIS A -- sign means team1 won (the documented contract):\n")
cat(sprintf("  agreement: %.1f%%\n", 100*mean(d$pos == d$team1_won)))
cat("HYPOTHESIS B -- sign means the side batting FIRST won:\n")
cat(sprintf("  agreement: %.1f%%\n", 100*mean(d$pos == d$batfirst_won)))
cat("\nhow often is team1 the side batting first?\n")
cat(sprintf("  %.1f%%\n", 100*mean(d$team1 == d$bat_first)))
