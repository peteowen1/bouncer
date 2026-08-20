# Does every function a pipeline script calls actually exist?
#
# get_calibration_data() was deleted on 2026-02-09 as dead code. Eight scripts
# called it, including the only one that populates the 3-way ELO tables, and
# nobody noticed for six months (bouncerverse#63). Nothing would have caught a
# second instance either: data-raw/ is outside R CMD check, so a function
# called only from pipeline scripts is invisible to every automated check and
# to grep over R/ alone.
#
# This is that check. Static only -- it parses, it does not run anything.
#
# Usage: Rscript data-raw/validation/pipeline_call_audit.R
suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))

ROOT <- "C:/dev/bouncerverse/bouncer/data-raw"
# _deprecated/ and archive/ are kept deliberately dead; they are not run.
# _deprecated/, archive/ and legacy/ are kept deliberately dead. debug/ is
# scratch and gitignored in spirit -- a broken debug script costs one session,
# a broken pipeline step costs six months.
SKIP <- c("_deprecated", "archive", "legacy", "debug")

files <- list.files(ROOT, pattern = "[.]R$", recursive = TRUE, full.names = TRUE)
files <- files[!grepl(paste0("/(", paste(SKIP, collapse = "|"), ")/"), files)]

# Everything reachable at run time: the package, plus every attached package a
# script could plausibly library().
pkgs <- c("bouncer", "base", "stats", "utils", "methods", "graphics", "grDevices",
          "tools", "DBI", "duckdb", "data.table", "dplyr", "tidyr", "purrr",
          "stringr", "lubridate", "cli", "glue", "xgboost", "arrow", "jsonlite",
          "ggplot2", "tibble", "readr", "zoo", "Matrix", "parallel", "devtools",
          "testthat", "qs", "piggyback", "httr", "httr2", "rvest", "fs", "rlang")
known <- unique(unlist(lapply(pkgs, function(p) {
  if (requireNamespace(p, quietly = TRUE)) {
    # all.names = TRUE, or every dot-prefixed internal (.competition_sql and
    # friends) reads as missing -- 20 false positives on the first run.
    unique(c(ls(asNamespace(p), all.names = TRUE),
             getNamespaceExports(asNamespace(p))))
  } else character(0)
})))

# Pipeline scripts source() each other, so a function defined in ANY data-raw
# file is reachable. Collect them all first rather than reporting a sibling's
# helper as missing.
sourced_defs <- unique(unlist(lapply(files, function(f) {
  pd <- tryCatch(utils::getParseData(parse(f, keep.source = TRUE)),
                 error = function(e) NULL)
  if (is.null(pd)) return(character(0))
  # `name <- function(` : the SYMBOL immediately preceding a LEFT_ASSIGN whose
  # right-hand side opens a function.
  idx <- which(pd$token == "FUNCTION")
  unlist(lapply(idx, function(i) {
    before <- pd[pd$line1 == pd$line1[i] & pd$token == "SYMBOL", "text"]
    if (length(before)) before[1] else character(0)
  }))
})))

unresolved <- list()
for (f in files) {
  pd <- tryCatch(utils::getParseData(parse(f, keep.source = TRUE)),
                 error = function(e) NULL)
  if (is.null(pd)) { unresolved[[f]] <- "PARSE ERROR"; next }
  calls <- unique(pd$text[pd$token == "SYMBOL_FUNCTION_CALL"])
  # Functions the file defines itself, and any it assigns to.
  local_defs <- unique(pd$text[pd$token == "SYMBOL"])
  # Tokens that are never bouncer functions: data.table/magrittr operators,
  # the pipe placeholder, and R6/chromote methods reached with $.
  NOISE <- c(".", ":=", "%>%", "%||%", "enable", "navigate", "s", "ti", "bam",
             "ocat", "font_add_google", "showtext_auto", "sticker")
  missing <- setdiff(calls, c(known, local_defs, sourced_defs, NOISE))
  if (length(missing)) unresolved[[f]] <- missing
}

cli::cli_h1("Pipeline call audit")
cli::cli_alert_info("{length(files)} script{?s} scanned, {length(unresolved)} with unresolved calls")
if (length(unresolved) == 0) {
  cli::cli_alert_success("Every call resolves.")
} else {
  for (f in names(unresolved)) {
    cat(sprintf("\n%s\n", sub(paste0(ROOT, "/"), "", f, fixed = TRUE)))
    cat(sprintf("  %s\n", paste(unresolved[[f]], collapse = ", ")))
  }
}
