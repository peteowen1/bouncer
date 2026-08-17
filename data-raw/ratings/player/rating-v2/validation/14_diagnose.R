# Two questions, both answerable by measurement rather than argument:
#
# Q1. Is Kohli at 171 a DECAY effect (his recent Test form, which the rating
#     deliberately weights) or an under-pricing of the competition he plays in?
#     The rating's stated target is "who would you rather have NEXT match", so
#     recent form is supposed to dominate. If his post-2020 Test average is
#     genuinely poor, 171 may be the rating being right and the ANCHOR being
#     wrong -- which is a finding about the anchor, not licence to tune.
#
# Q2. Are County Championship players systematically over-rated relative to
#     Test players? 11 of the top 20 on a 1.073 competition factor.
suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
DB <- "C:/dev/bouncerverse/bouncerdata/bouncer.duckdb"
stopifnot(file.exists(DB), file.info(DB)$size > 1e10)
conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = DB, read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
OUT <- "C:/Users/peteo/AppData/Local/Temp/claude/C--dev-bouncerverse/635fc43f-1352-411b-8c7d-693d0ebc00b2/scratchpad/test_lambda"

cat("=== Q1: the four anchors' TEST-ONLY batting average by era ===\n")
cat("    (the rating decays toward now, so what matters is the recent block)\n\n")
ids <- list()
for (q in c("JE Root","V Kohli","SPD Smith","KS Williamson")) {
  f <- find_player(q, conn = conn, quiet = TRUE); ids[[q]] <- f$player_id[1]
}
era <- rbindlist(lapply(names(ids), function(q) {
  as.data.table(DBI::dbGetQuery(conn, sprintf("
    SELECT CASE WHEN d.match_date < DATE '2020-01-01' THEN 'pre-2020'
                ELSE '2020 onward' END AS era,
           COUNT(*) AS balls,
           SUM(d.runs_batter) AS runs,
           SUM(CASE WHEN d.player_out_id = d.batter_id THEN 1 ELSE 0 END) AS outs
    FROM cricsheet.deliveries d
    WHERE d.batter_id = '%s' AND LOWER(d.match_type)='test' AND d.gender='male'
    GROUP BY 1", ids[[q]])))[, player := q]
}))
era[, avg := round(runs / pmax(outs, 1), 1)]
print(dcast(era, player ~ era, value.var = c("avg", "balls")))

cat("\n=== Q2: mean rating by competition, and how the top of the list splits ===\n")
r <- readRDS(file.path(OUT, "rating_test_batter.rds"))
setDT(r)
print(r[, .(players = .N,
            mean_rating = round(mean(rating), 2),
            mean_average = round(mean(average, na.rm = TRUE), 1),
            mean_eff_mts = round(mean(effective_matches), 1)),
        by = main_comp][order(-mean_rating)])

cat("\n  share of each band that is Test-primary:\n")
for (k in c(20, 50, 100, 200)) {
  s <- r[rank <= k]
  cat(sprintf("    top %3d: %4.1f%% Test-primary,  %4.1f%% County\n", k,
              100 * mean(s$main_comp == "Test"),
              100 * mean(s$main_comp == "County Championship")))
}
cat(sprintf("    overall: %4.1f%% Test-primary,  %4.1f%% County\n",
            100*mean(r$main_comp == "Test"), 100*mean(r$main_comp == "County Championship")))

cat("\n=== Q2b: do County-primary players have a HIGHER rating at equal average? ===\n")
cat("    (if the competition factor were right, rating should track average\n")
cat("     similarly in both; a gap means county runs are being over-credited)\n")
sub <- r[main_comp %in% c("Test", "County Championship") &
           !is.na(average) & effective_matches >= 20]
m <- lm(rating ~ average + main_comp, data = sub)
print(round(coef(summary(m)), 3))
cat(sprintf("\n  n = %d (Test %d, County %d)\n", nrow(sub),
            sub[main_comp == "Test", .N], sub[main_comp == "County Championship", .N]))
cat("  A positive County coefficient = a county player with the SAME average\n")
cat("  rates HIGHER. That is the competition factor being too small.\n")

cat("\n=== Q2c: implied factor needed to equalise them ===\n")
cf <- coef(m)
if ("main_compCounty Championship" %in% names(cf)) {
  gap <- cf[["main_compCounty Championship"]]
  base <- mean(sub[main_comp == "County Championship", rating])
  cat(sprintf("  county premium at equal average: %+.2f rating points\n", gap))
  cat(sprintf("  mean county rating %.2f -> implied extra discount ~%.3fx\n",
              base, base / (base - gap)))
  cat(sprintf("  current County factor is 1.073; this suggests ~%.2f\n",
              1.073 * base / (base - gap)))
}
