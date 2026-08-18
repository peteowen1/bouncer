suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
DB <- "C:/dev/bouncerverse/bouncerdata/bouncer.duckdb"
stopifnot(file.exists(DB), file.info(DB)$size > 1e10)
conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = DB, read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

OUT <- "C:/Users/peteo/AppData/Local/Temp/claude/C--dev-bouncerverse/635fc43f-1352-411b-8c7d-693d0ebc00b2/scratchpad/test_lambda"

# Pre-declared anchors, from TEST-LAMBDA-PREDECLARATION.md / the scope doc.
BAT_ANCHORS <- c("JE Root", "V Kohli", "SPD Smith", "KS Williamson")

for (role in c("batter", "bowler")) {
  cat("\n", strrep("=", 70), "\n", toupper(role), "\n", strrep("=", 70), "\n", sep = "")
  t0 <- Sys.time()
  r <- tryCatch(
    calculate_player_rating_v2(format = "test", gender = "male",
                               role = role, conn = conn),
    error = function(e) { cat("ERROR:", conditionMessage(e), "\n"); NULL })
  if (is.null(r)) next
  cat(sprintf("elapsed %.1f min | rated %d players\n",
              as.numeric(difftime(Sys.time(), t0, units = "mins")), nrow(r)))
  r <- as.data.table(r)
  saveRDS(r, file.path(OUT, sprintf("rating_test_%s.rds", role)))

  cat("\n--- top 25 ---\n")
  cols <- intersect(c("rank","player_name","rating","average","main_comp",
                      "matches","balls","effective_matches"), names(r))
  print(r[1:min(25, .N), ..cols])

  if (role == "batter") {
    cat("\n--- ANCHOR CHECK (pre-declared: top 15-20 of the qualifying pool) ---\n")
    pass <- 0
    for (q in BAT_ANCHORS) {
      f <- tryCatch(find_player(q, conn = conn, quiet = TRUE), error = function(e) NULL)
      if (is.null(f) || !nrow(f)) { cat(sprintf("  %-16s NOT FOUND\n", q)); next }
      row <- r[player_id == f$player_id[1]]
      if (!nrow(row)) { cat(sprintf("  %-16s not in the qualifying pool\n", q)); next }
      ok <- row$rank <= 20
      pass <- pass + ok
      cat(sprintf("  %-16s rank %4d / %d  rating %+6.2f  avg %5.1f  eff.mts %5.1f  %s\n",
                  q, row$rank, nrow(r), row$rating, row$average,
                  row$effective_matches, if (ok) "PASS" else "MISS"))
    }
    cat(sprintf("  --> %d of %d anchors inside the top 20\n", pass, length(BAT_ANCHORS)))

    cat("\n--- domestic contamination in the top 20 (was 10/20 on raw RAA) ---\n")
    print(r[1:20, .N, by = main_comp][order(-N)])
  } else {
    cat("\n--- bowling face check: recognisable Test attack leaders near the top? ---\n")
    for (q in c("R Ashwin", "PJ Cummins", "JM Anderson", "DW Steyn", "N Wagner")) {
      f <- tryCatch(find_player(q, conn = conn, quiet = TRUE), error = function(e) NULL)
      if (is.null(f) || !nrow(f)) { cat(sprintf("  %-16s NOT FOUND\n", q)); next }
      row <- r[player_id == f$player_id[1]]
      if (!nrow(row)) { cat(sprintf("  %-16s not in the qualifying pool\n", q)); next }
      cat(sprintf("  %-16s rank %4d / %d  rating %+6.2f  avg %5.1f  main %s\n",
                  q, row$rank, nrow(r), row$rating, row$average, row$main_comp))
    }
  }
}
