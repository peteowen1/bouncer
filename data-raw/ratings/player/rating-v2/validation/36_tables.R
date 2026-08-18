suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
source("C:/Users/peteo/AppData/Local/Temp/claude/C--dev-bouncerverse/635fc43f-1352-411b-8c7d-693d0ebc00b2/scratchpad/test_lambda/_context.R")
conn <- DBI::dbConnect(duckdb::duckdb(),
  dbdir="C:/dev/bouncerverse/bouncerdata/bouncer.duckdb", read_only=TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown=TRUE), add=TRUE)
id_map <- build_player_id_map(conn)
ctx <- player_context(conn)

rat <- list()
for (mt in c("runs","wickets","team_score","composite")) {
  r <- suppressMessages(calculate_player_rating_v2("t20","male", role="batter",
        conn=conn, id_map=id_map, metric=mt))
  rat[[mt]] <- with_context(r, ctx)
}

show <- function(mt, lab) {
  cat("\n", strrep("=",84), "\n T20 MALE -- ", lab, "\n", strrep("=",84), "\n", sep="")
  d <- rat[[mt]][order(rank)][1:12]
  cat(sprintf("  %-4s %-19s %-13s %-24s %8s %6s\n",
              "rank","player","country","modal league","rating","ICC"))
  cat("  ", strrep("-",80), "\n", sep="")
  for (i in 1:nrow(d)) cat(sprintf("  %-4d %-19s %-13s %-24s %8.2f %6s\n",
    d$rank[i], substr(d$player_name[i],1,19),
    substr(ifelse(is.na(d$country[i]),"-",d$country[i]),1,13),
    substr(ifelse(is.na(d$main_comp[i]),"-",d$main_comp[i]),1,24),
    d$rating[i], ifelse(is.na(d$icc[i]),"-",as.character(d$icc[i]))))
}
show("composite",  "COMPOSITE rating")
show("runs",       "RUNS rating")
show("wickets",    "WICKETS rating")
show("team_score", "TEAM SCORE rating")

cat("\n\n", strrep("=",84), "\n IS THE COMPETITION ADJUSTMENT LEAVING A LEAGUE BIAS?\n",
    strrep("=",84), "\n", sep="")
cat(" If the adjustment worked, mean rating should be similar across modal leagues.\n")
cat(" A league whose players sit systematically high is under-discounted.\n\n")
d <- rat$composite[!is.na(main_comp)]
agg <- d[, .(players = .N, mean_rating = round(mean(rating),2),
             median_rank = as.integer(median(rank)),
             in_top100 = sum(rank <= 100)), by = main_comp][players >= 15]
setorder(agg, -mean_rating)
cat(sprintf("  %-30s %8s %12s %12s %9s\n","modal league","players","mean rating","median rank","top-100"))
for (i in 1:nrow(agg)) cat(sprintf("  %-30s %8d %12.2f %12d %9d\n",
  substr(agg$main_comp[i],1,30), agg$players[i], agg$mean_rating[i],
  agg$median_rank[i], agg$in_top100[i]))
