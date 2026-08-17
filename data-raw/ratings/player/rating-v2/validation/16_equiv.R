# The as_at fix must not move the SHIPPED ratings. Three checks:
#   1. as_at = NULL reproduces the rating computed before the change.
#   2. as_at = <max match date> is a no-op -- identical to as_at = NULL. This is
#      the one that actually exercises the new branch.
#   3. as_at = an earlier date genuinely restricts (fewer players, different order).
suppressMessages(devtools::document("C:/dev/bouncerverse/bouncer"))
suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
DB <- "C:/dev/bouncerverse/bouncerdata/bouncer.duckdb"
stopifnot(file.exists(DB), file.info(DB)$size > 1e10)
conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = DB, read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
OUT <- "C:/Users/peteo/AppData/Local/Temp/claude/C--dev-bouncerverse/635fc43f-1352-411b-8c7d-693d0ebc00b2/scratchpad/test_lambda"

old <- as.data.table(readRDS(file.path(OUT, "rating_test_batter.rds")))

cat("=== 1. as_at = NULL vs the pre-change saved rating ===\n")
a <- as.data.table(calculate_player_rating_v2("test", "male", role = "batter", conn = conn))
cat(sprintf("  players %d vs %d\n", nrow(a), nrow(old)))
cmp <- merge(a[, .(player_id, new = rating)], old[, .(player_id, ref = rating)], by = "player_id")
cat(sprintf("  matched %d, max |diff| %.3e, identical order: %s\n", nrow(cmp),
            max(abs(cmp$new - cmp$ref)),
            identical(a$player_id, old$player_id)))

cat("\n=== 2. as_at = max match date must be a NO-OP (exercises the new branch) ===\n")
mx <- DBI::dbGetQuery(conn, "
  SELECT MAX(match_date) AS d FROM main.cricsheet_ball_raa
  WHERE format='TEST' AND gender='male'")$d
cat("  max match_date:", format(mx), "\n")
b <- as.data.table(calculate_player_rating_v2("test", "male", role = "batter",
                                              conn = conn, as_at = mx))
cat(sprintf("  players %d\n", nrow(b)))
cmp2 <- merge(a[, .(player_id, null_at = rating)], b[, .(player_id, max_at = rating)], by = "player_id")
cat(sprintf("  matched %d, max |diff| %.3e, identical order: %s\n", nrow(cmp2),
            max(abs(cmp2$null_at - cmp2$max_at)),
            identical(a$player_id, b$player_id)))
if (max(abs(cmp2$null_at - cmp2$max_at)) > 1e-9)
  cat("  *** NOT a no-op -- the new branch changes the default answer ***\n")

cat("\n=== 3. an earlier as_at must genuinely restrict ===\n")
c20 <- as.data.table(calculate_player_rating_v2("test", "male", role = "batter",
                                                conn = conn, as_at = as.Date("2020-01-01")))
cat(sprintf("  as_at 2020-01-01: %d players (vs %d at latest)\n", nrow(c20), nrow(a)))
cat("  top 5 as at 2020-01-01:\n")
print(c20[1:5, .(rank, player_name, rating, average, main_comp)])
