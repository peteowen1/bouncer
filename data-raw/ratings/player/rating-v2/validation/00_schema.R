suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
library(DBI)
conn <- get_db_connection(read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

cat("=== cricsheet.matches columns ===\n")
m <- dbGetQuery(conn, "SELECT * FROM cricsheet.matches LIMIT 1")
print(names(m))

cat("\n=== cricsheet.deliveries columns ===\n")
d <- dbGetQuery(conn, "SELECT * FROM cricsheet.deliveries LIMIT 1")
print(names(d))

cat("\n=== Test/MDM match counts + outcome fields ===\n")
print(dbGetQuery(conn, "
  SELECT match_type, gender, outcome_type, COUNT(*) AS matches
  FROM cricsheet.matches
  WHERE match_type IN ('Test','MDM')
  GROUP BY 1,2,3 ORDER BY 1,2,3"))

cat("\n=== what outcome_* columns look like on Test ===\n")
print(dbGetQuery(conn, "
  SELECT match_id, team1, team2, winner, outcome_type,
         outcome_by_runs, outcome_by_wickets, match_date
  FROM cricsheet.matches
  WHERE match_type = 'Test' AND gender = 'male'
  ORDER BY match_date DESC LIMIT 8"))

cat("\n=== innings structure: how many innings per Test, and batting_team present? ===\n")
print(dbGetQuery(conn, "
  SELECT n_inn, COUNT(*) AS matches FROM (
    SELECT d.match_id, COUNT(DISTINCT d.innings) AS n_inn
    FROM cricsheet.deliveries d
    JOIN cricsheet.matches m ON m.match_id = d.match_id
    WHERE d.match_type = 'Test' AND d.gender = 'male'
    GROUP BY d.match_id
  ) GROUP BY 1 ORDER BY 1"))

cat("\n=== deliveries: state columns available on Test ===\n")
print(dbGetQuery(conn, "
  SELECT innings, over, ball, batting_team, bowling_team,
         runs_total, total_runs, wickets_fallen, wicket_kind
  FROM cricsheet.deliveries
  WHERE match_type='Test' AND gender='male' AND innings=4
  ORDER BY match_id, delivery_id LIMIT 6"))
