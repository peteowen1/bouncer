# Rebuild every 3-way ELO table, one at a time (bouncerverse#63).
#
# STRICTLY SEQUENTIAL, and not merely for tidiness: DuckDB allows one write
# connection, so two rebuilds in parallel do not run twice as fast -- one of
# them fails to connect. Each format is also chronological internally and
# cannot be parallelised within itself.
#
# Safe to interrupt. Each format builds into its own staging table and is
# promoted only when complete, so a kill costs the format in flight and
# nothing already promoted. Re-running repeats only what did not finish.
#
# Usage:
#   Rscript data-raw/ratings/player/3way-elo/rebuild_all.R
#   Rscript data-raw/ratings/player/3way-elo/rebuild_all.R mens_odi mens_test

HERE <- dirname(normalizePath(sub("^--file=", "",
  commandArgs(trailingOnly = FALSE)[grep("^--file=",
    commandArgs(trailingOnly = FALSE))][1])))

ALL <- list(c("mens", "t20"), c("womens", "t20"),
            c("mens", "odi"), c("womens", "odi"),
            c("mens", "test"), c("womens", "test"))

wanted <- commandArgs(trailingOnly = TRUE)
if (length(wanted)) {
  ALL <- Filter(function(x) paste(x, collapse = "_") %in% wanted, ALL)
  if (!length(ALL)) stop("No matching format; use e.g. mens_odi womens_test")
}

log_line <- function(...) {
  cat(sprintf("[%s] %s\n", format(Sys.time(), "%H:%M:%S"), paste0(...)))
  flush.console()
}

results <- list()
for (job in ALL) {
  tag <- paste(job, collapse = "_")
  log_line("START ", tag)
  # Snapshot per-player final ratings BEFORE the rebuild. The promote drops the
  # live table, so without this there is nothing left to compare a rebuild
  # against. One row per player, not the whole table.
  snap <- system2("Rscript",
    c(shQuote(file.path(HERE, "..", "..", "..", "validation",
                        "snapshot_3way_elo_ratings.R")), job[1], job[2]))
  if (snap != 0) log_line("WARN  snapshot failed for ", tag,
                          " -- rebuilding anyway, but old-vs-new will not be possible")
  t0 <- Sys.time()
  # A separate process per format, deliberately: the calculation script sets
  # globals and setwd()s, and a failure in one format must not poison the next.
  status <- system2("Rscript",
    c(shQuote(file.path(HERE, "01_calculate_3way_elo.R")), job[1], job[2]))
  mins <- round(as.numeric(difftime(Sys.time(), t0, units = "mins")), 1)
  results[[tag]] <- list(status = status, mins = mins)
  log_line(if (status == 0) "OK    " else "FAILED", " ", tag, " (", mins, " mins)")
}

cat("\n==== summary ====\n")
for (tag in names(results)) {
  r <- results[[tag]]
  cat(sprintf("%-14s %-7s %6.1f mins\n", tag,
              if (r$status == 0) "ok" else paste0("exit ", r$status), r$mins))
}
failed <- names(Filter(function(r) r$status != 0, results))
if (length(failed)) {
  # Named, not counted: a silently skipped format is a silently stale table.
  cat("\nFAILED and NOT promoted: ", paste(failed, collapse = ", "), "\n")
  quit(status = 1)
}
