# Derive the run-rate shrinkage weight per format.
#
# Variance of the observed rate after n balls decomposes as
#   sd^2(n) = B + N/n
# with B the between-innings variance of the TRUE rate and N the per-ball noise.
# The MSE-optimal shrinkage weight, in balls, is k = N / B.
setwd("C:/dev/bouncerverse/bouncer")
suppressMessages(devtools::load_all(quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
conn <- get_db_connection(read_only = TRUE); on.exit(dbDisconnect(conn, shutdown=TRUE))
d <- as.data.table(dbGetQuery(conn, "
  WITH b AS (
    SELECT d.match_id, d.innings, d.over*6 + d.ball AS bn, d.runs_total,
           CASE WHEN LOWER(m.match_type) IN ('t20','it20') THEN 't20'
                WHEN LOWER(m.match_type) IN ('odi','odm') THEN 'odi' ELSE 'test' END AS fmt
    FROM cricsheet.deliveries d JOIN cricsheet.matches m ON m.match_id = d.match_id
    WHERE COALESCE(m.balls_per_over,6)=6),
  c AS (SELECT fmt, match_id, innings, bn,
               SUM(runs_total) OVER (PARTITION BY match_id, innings ORDER BY bn) AS cum FROM b)
  SELECT fmt, bn, 6.0*cum/bn AS rate FROM c WHERE bn IN (6,12,24,60,120,180)"))
cat(sprintf("%-6s %8s %8s %8s %8s\n", "format", "N(noise)", "B(true)", "k=N/B", "prior"))
for (f in c("t20","odi","test")) {
  s <- d[fmt == f, .(v = var(rate, na.rm = TRUE)), by = bn][order(bn)]
  s[, inv := 1/bn]
  fit <- lm(v ~ inv, data = s)          # v = B + N * (1/n)
  B <- coef(fit)[[1]]; N <- coef(fit)[[2]]
  k <- N / B
  pr <- d[fmt == f & bn == 180, mean(rate, na.rm = TRUE)]
  if (is.na(pr)) pr <- d[fmt == f & bn == 120, mean(rate, na.rm = TRUE)]
  cat(sprintf("%-6s %8.2f %8.3f %8.1f %8.2f\n", f, N, B, k, pr))
}
