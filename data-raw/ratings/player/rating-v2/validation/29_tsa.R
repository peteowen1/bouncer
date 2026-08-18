# L2: Team Score Added (TSA) -- the player's per-ball effect on his team's
# projected final score, net of what an average player would have done.
#
#   proj_before   projection from the PRE-delivery state
#   proj_after    projection from the POST-delivery state (actual outcome)
#   proj_expected projection from the state an AVERAGE ball would have produced,
#                 using the agnostic model's exp_runs and exp_wicket
#   tsa = proj_after - proj_expected
#
# Same shape as RAA and WAA -- actual minus expected -- but denominated in
# projected final runs, so it prices TEMPO: a dot ball consumes a ball of
# resource without adding runs, which an average ball would not have done.
#
# Limited overs only. The resource model needs a fixed ball allocation, and Test
# has none -- the same reason `overs_left` is absent from the Test agnostic model.
suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
DB <- "C:/dev/bouncerverse/bouncerdata/bouncer.duckdb"
stopifnot(file.exists(DB), file.info(DB)$size > 1e10)
conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = DB, read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

FMT <- "t20"; GEN <- "male"; MAXB <- 120L
p <- tryCatch(load_projection_params(FMT, GEN, "international", conn = conn),
              error = function(e) { cat("params load failed:", conditionMessage(e), "\n"); NULL })
if (is.null(p)) quit(status = 1)
cat("params:", paste(names(p), round(unlist(p[sapply(p, is.numeric)]), 4),
                     sep = "=", collapse = "  "), "\n")

# innings 1 only: a chase truncates the innings, so "projected final score" is
# not the quantity being predicted once the target is passed.
d <- as.data.table(DBI::dbGetQuery(conn, sprintf("
  SELECT r.delivery_id, r.match_id, r.match_date, r.batter_id, r.bowler_id,
         r.over_number, r.ball_number, r.actual_runs, CAST(r.is_wicket AS INT) AS is_wicket,
         r.exp_runs, r.exp_wicket,
         d.total_runs - d.runs_total AS runs_pre,
         d.wickets_fallen - CAST(d.is_wicket AS INT) AS wkts_pre
  FROM main.cricsheet_ball_raa r
  JOIN cricsheet.deliveries d ON d.delivery_id = r.delivery_id
  WHERE r.format='%s' AND r.gender='male' AND r.innings_number = 1", toupper(FMT))))
cat(sprintf("deliveries: %s\n", format(nrow(d), big.mark = ",")))

d[, balls_bowled := over_number * 6L + ball_number]
d[, balls_rem_pre := pmax(0L, MAXB - balls_bowled + 1L)]
d[, balls_rem_post := pmax(0L, MAXB - balls_bowled)]

proj <- function(score, wkts_rem, balls_rem) {
  calculate_projected_scores_vectorized(
    current_score = score, wickets_remaining = wkts_rem,
    balls_remaining = balls_rem, expected_initial_score = p$eis_agnostic,
    a = p$a, b = p$b, z = p$z, y = p$y, max_balls = MAXB)
}

d[, proj_before   := proj(runs_pre, 10L - wkts_pre, balls_rem_pre)]
d[, proj_actual   := proj(runs_pre + actual_runs, 10L - wkts_pre - is_wicket, balls_rem_post)]
d[, proj_expected := proj(runs_pre + exp_runs, 10 - wkts_pre - exp_wicket, balls_rem_post)]
d[, tsa := proj_actual - proj_expected]

cat("\n=== TSA sanity ===\n")
cat(sprintf("  mean %.5f  sd %.3f  (mean should be ~0 by construction)\n",
            mean(d$tsa, na.rm = TRUE), sd(d$tsa, na.rm = TRUE)))
cat(sprintf("  mean on a DOT ball        %+.3f   <- tempo cost: a ball used, no runs\n",
            d[actual_runs == 0 & is_wicket == 0, mean(tsa, na.rm = TRUE)]))
cat(sprintf("  mean on a BOUNDARY (4/6)  %+.3f\n", d[actual_runs >= 4, mean(tsa, na.rm = TRUE)]))
cat(sprintf("  mean on a WICKET          %+.3f\n", d[is_wicket == 1, mean(tsa, na.rm = TRUE)]))

agg <- d[, .(balls = .N, tsa100 = 100 * mean(tsa, na.rm = TRUE),
             sr = 100 * mean(actual_runs)), by = batter_id][balls >= 1500]
nm <- as.data.table(DBI::dbGetQuery(conn, "
  SELECT player_id AS batter_id, ANY_VALUE(player_name) AS player
  FROM cricsheet.players GROUP BY player_id"))
agg <- merge(agg, nm, by = "batter_id", all.x = TRUE)

cat("\n=== TOP 10 by Team Score Added per 100 balls ===\n")
print(agg[order(-tsa100)][1:10, .(player, tsa100 = round(tsa100,1), sr = round(sr,1), balls)])
cat("\n=== BOTTOM 10 -- the tempo cost the other two metrics miss ===\n")
print(agg[order(tsa100)][1:10, .(player, tsa100 = round(tsa100,1), sr = round(sr,1), balls)])
saveRDS(d[, .(delivery_id, batter_id, bowler_id, match_id, match_date, tsa)],
        "C:/Users/peteo/AppData/Local/Temp/claude/C--dev-bouncerverse/635fc43f-1352-411b-8c7d-693d0ebc00b2/scratchpad/test_lambda/tsa_t20.rds")
