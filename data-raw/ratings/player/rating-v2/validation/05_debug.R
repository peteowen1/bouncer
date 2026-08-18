suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages(library(DBI))
conn <- get_db_connection(read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

p <- function(lbl, sql) {
  cat("\n===", lbl, "===\n")
  r <- tryCatch(dbGetQuery(conn, sql), error = function(e) {
    cat("  ERROR:", conditionMessage(e), "\n"); NULL })
  if (!is.null(r)) print(r)
}

p("A. inn CTE row count",
  "SELECT COUNT(*) AS n FROM (
     SELECT match_id, innings, batting_team, SUM(runs_total) AS inn_total
     FROM cricsheet.deliveries
     WHERE match_type IN ('Test','MDM') AND gender='male'
     GROUP BY 1,2,3)")

p("B. base deliveries count with the join",
  "SELECT COUNT(*) AS n
   FROM cricsheet.deliveries d
   JOIN cricsheet.matches m ON m.match_id = d.match_id
   WHERE d.match_type IN ('Test','MDM') AND d.gender='male'")

p("C. wkts_pre distribution (is the 0..9 filter killing rows?)",
  "SELECT MIN(w) AS min_w, MAX(w) AS max_w,
          SUM(CASE WHEN w BETWEEN 0 AND 9 THEN 1 ELSE 0 END) AS in_range,
          COUNT(*) AS n
   FROM (SELECT wickets_fallen - CASE WHEN is_wicket THEN 1 ELSE 0 END AS w
         FROM cricsheet.deliveries
         WHERE match_type IN ('Test','MDM') AND gender='male')")

p("D. is match_type on MATCHES also 'Test'/'MDM'? (join could be fine but filter on d only)",
  "SELECT match_type, COUNT(*) AS n FROM cricsheet.matches
   WHERE match_type IN ('Test','MDM') GROUP BY 1")

p("E. the 'd' CTE alone, 5 rows -- does ROW_NUMBER + outcome survive?",
  "WITH d AS (
     SELECT dd.match_id, dd.innings, dd.batting_team,
       dd.total_runs - dd.runs_total AS runs_pre,
       dd.wickets_fallen - CASE WHEN dd.is_wicket THEN 1 ELSE 0 END AS wkts_pre,
       ROW_NUMBER() OVER (PARTITION BY dd.match_id ORDER BY dd.innings, dd.over, dd.ball) - 1 AS balls_elapsed,
       m.outcome_type, m.outcome_winner
     FROM cricsheet.deliveries dd
     JOIN cricsheet.matches m ON m.match_id = dd.match_id
     WHERE dd.match_type IN ('Test','MDM') AND dd.gender='male')
   SELECT * FROM d LIMIT 5")

p("F. d CTE count",
  "WITH d AS (
     SELECT dd.match_id, dd.innings, dd.batting_team,
       dd.wickets_fallen - CASE WHEN dd.is_wicket THEN 1 ELSE 0 END AS wkts_pre
     FROM cricsheet.deliveries dd
     JOIN cricsheet.matches m ON m.match_id = dd.match_id
     WHERE dd.match_type IN ('Test','MDM') AND dd.gender='male')
   SELECT COUNT(*) AS n, MIN(wkts_pre) AS mn, MAX(wkts_pre) AS mx FROM d")
