suppressMessages(devtools::document("C:/dev/bouncerverse/bouncer"))
suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
source("C:/Users/peteo/AppData/Local/Temp/claude/C--dev-bouncerverse/635fc43f-1352-411b-8c7d-693d0ebc00b2/scratchpad/test_lambda/_context.R")
conn <- DBI::dbConnect(duckdb::duckdb(),
  dbdir="C:/dev/bouncerverse/bouncerdata/bouncer.duckdb", read_only=TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown=TRUE), add=TRUE)
id_map <- build_player_id_map(conn); ctx <- player_context(conn)

r <- with_context(suppressMessages(calculate_player_rating_v2("t20","male",
       role="batter", conn=conn, id_map=id_map, metric="composite")), ctx)
setorder(r, rank)   # merge() in with_context() drops the ordering
cat("\n=== T20 MALE COMPOSITE, after merging sponsor variants ===\n")
cat(sprintf("  %-4s %-19s %-13s %-24s %8s %5s\n","rank","player","country","modal league","rating","ICC"))
for (i in 1:12) cat(sprintf("  %-4d %-19s %-13s %-24s %8.2f %5s\n", r$rank[i],
  substr(r$player_name[i],1,19), substr(ifelse(is.na(r$country[i]),"-",r$country[i]),1,13),
  substr(ifelse(is.na(r$main_comp[i]),"-",r$main_comp[i]),1,24), r$rating[i],
  ifelse(is.na(r$icc[i]),"-",as.character(r$icc[i]))))

cat("\n=== did English-domestic players move? ===\n")
eng <- r[main_comp %in% c("Vitality Blast")][order(rank)]
cat(sprintf("  Vitality Blast players rated: %d, best rank %d, median rank %d\n",
            nrow(eng), min(eng$rank), as.integer(median(eng$rank))))
cat("  top 5 English-domestic-primary:\n")
for (i in 1:min(5,nrow(eng))) cat(sprintf("    %-4d %-20s %6.2f\n",
  eng$rank[i], substr(eng$player_name[i],1,20), eng$rating[i]))
saveRDS(r, "C:/Users/peteo/AppData/Local/Temp/claude/C--dev-bouncerverse/635fc43f-1352-411b-8c7d-693d0ebc00b2/scratchpad/test_lambda/after_alias.rds")
