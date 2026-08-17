# Shared preamble. Pin the database explicitly and PROVE it has rows.
#
# get_db_connection() resolves the path via find_bouncerdata_dir(), which walks
# up for a bouncerdata/ sibling and otherwise falls back to the rappdirs user
# data dir. That fallback holds a 5MB stub with the correct SCHEMA and ZERO
# ROWS, so from any cwd outside the repo every query returns 0 rows with no
# warning -- indistinguishable from "this format has no data".
suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(arrow)})

DB   <- "C:/dev/bouncerverse/bouncerdata/bouncer.duckdb"
OUT  <- "C:/Users/peteo/AppData/Local/Temp/claude/C--dev-bouncerverse/635fc43f-1352-411b-8c7d-693d0ebc00b2/scratchpad/test_lambda"

stopifnot(file.exists(DB))
if (file.info(DB)$size < 1e10) {
  stop("Database at ", DB, " is only ", file.info(DB)$size,
       " bytes -- that is the empty stub, not the real 20GB corpus.")
}

conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = DB, read_only = TRUE)

# Never trust the connection: assert the corpus is actually there.
.n_matches <- DBI::dbGetQuery(conn, "SELECT COUNT(*) n FROM cricsheet.matches")$n
.n_test <- DBI::dbGetQuery(conn, "
  SELECT COUNT(*) n FROM cricsheet.deliveries
  WHERE match_type IN ('Test','MDM') AND gender='male'")$n
if (.n_matches < 20000 || .n_test < 5e6) {
  stop("Corpus assertion failed: matches=", .n_matches, " test+mdm balls=", .n_test)
}
cat(sprintf("DB ok: %s matches, %s Test+MDM male deliveries\n",
            format(.n_matches, big.mark = ","), format(.n_test, big.mark = ",")))
