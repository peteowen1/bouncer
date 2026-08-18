# Where do the ICC T20I top 20 land in each of our four T20 ratings?
#
# SOFT reference by construction: ICC ranks T20I only, our bucket pools all T20
# including franchise leagues, and ICC's formula decays hard on recent
# internationals. A buried ICC player is a flag to investigate, never a target
# to fit to.
suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
DB <- "C:/dev/bouncerverse/bouncerdata/bouncer.duckdb"
stopifnot(file.exists(DB), file.info(DB)$size > 1e10)
conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = DB, read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
id_map <- build_player_id_map(conn)

icc <- fread("C:/dev/bouncerverse/docs/reference/icc-rankings-2026-08.csv")
top <- icc[format == "t20i" & discipline == "batting" & rank <= 20]
cat(sprintf("ICC T20I batting top 20: %d rows\n", nrow(top)))

rat <- list()
for (mt in c("runs","wickets","team_score","composite")) {
  r <- tryCatch(suppressMessages(calculate_player_rating_v2(
        "t20","male", role="batter", conn=conn, id_map=id_map, metric=mt)),
       error=function(e) NULL)
  if (!is.null(r)) rat[[mt]] <- as.data.table(r)[, .(player_id, player_name, rank, rating)]
}

# match ICC full names to cricsheet initials-style names
short <- function(full) {
  parts <- strsplit(trimws(full), " +")[[1]]
  if (length(parts) < 2) return(full)
  paste0(substr(parts[1],1,1), " ", paste(parts[-1], collapse=" "))
}

# Explicit aliases where the initials form is not derivable from the full name.
# Selecting a fuzzy match by ALL-FORMAT volume picked the wrong player twice:
# "Brian Bennett" -> HK Bennett (20 T20 balls, but a long ODI/Test career) over
# BJ Bennett (1,260), and "Suryakumar Yadav" -> "S Yadav" -> UT Yadav. A name is
# not an identifier; where it is ambiguous it has to be stated.
ALIAS <- c("Suryakumar Yadav" = "SA Yadav", "Brian Bennett" = "BJ Bennett")

out <- data.table()
for (i in seq_len(nrow(top))) {
  nm <- top$player_name[i]
  f <- NULL
  cands <- if (nm %in% names(ALIAS)) ALIAS[[nm]] else unique(c(nm, short(nm)))
  for (cand in cands) {
    hit <- tryCatch(find_player(cand, conn=conn, quiet=TRUE), error=function(e) NULL)
    if (!is.null(hit) && nrow(hit)) { f <- hit[which.max(hit$balls), ]; break }
  }
  row <- data.table(icc_rank = top$rank[i], player = nm,
                    matched = if (is.null(f)) NA_character_ else f$player_name)
  for (mt in names(rat)) {
    v <- if (is.null(f)) NA_integer_ else rat[[mt]][player_id == f$player_id, rank][1]
    row[[mt]] <- if (length(v) && !is.na(v)) v else NA_integer_
  }
  out <- rbind(out, row, fill = TRUE)
}
cat("
")
cat(sprintf("  %-3s %-20s %-18s %6s %8s %10s %10s
",
            "ICC","player","matched as","runs","wickets","teamscore","composite"))
cat("  ", strrep("-", 80), "
", sep="")
for (i in seq_len(nrow(out))) {
  f <- function(x) if (is.na(x)) "  --" else format(x)
  cat(sprintf("  %-3d %-20s %-18s %6s %8s %10s %10s
",
      out$icc_rank[i], substr(out$player[i],1,20),
      substr(ifelse(is.na(out$matched[i]),"NOT FOUND",out$matched[i]),1,18),
      f(out$runs[i]), f(out$wickets[i]), f(out$team_score[i]), f(out$composite[i])))
}

cat("\n=== how well does each rating agree with ICC's ordering? ===\n")
for (mt in names(rat)) {
  o <- out[!is.na(get(mt))]
  cat(sprintf("  %-11s matched %2d/20   spearman vs ICC rank %+.3f   median our-rank %s\n",
              mt, nrow(o), if (nrow(o) > 3) cor(o$icc_rank, o[[mt]], method="spearman") else NA_real_,
              format(median(o[[mt]]))))
}
cat("\n  A high spearman would mean we reproduce ICC. We should NOT expect that --\n")
cat("  different population (franchise included) and different decay. What matters\n")
cat("  is whether ICC's top 20 land broadly near the top of ours, not the order.\n")
