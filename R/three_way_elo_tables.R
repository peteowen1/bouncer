# Where the 3-way ELO actually lives.
#
# The rating tables are keyed by gender AND format -- `mens_t20_3way_elo`,
# `womens_odi_3way_elo` -- because `01_calculate_3way_elo.R` writes one table
# per (gender, format) pair. Two production readers instead built the name as
# `paste0(format, "_3way_elo")`, which resolves to a DIFFERENT, unpopulated set
# of tables (bouncerverse#63).
#
# What that cost: `t20_3way_elo` is empty, so the full outcome model joined
# every T20 delivery against nothing and coalesced all three ELO features to
# neutral -- for every row, silently. `odi_3way_elo` and `test_3way_elo` hold
# stale copies of the WOMEN'S ratings, and since delivery ids are unique per
# match, a men's delivery matches no row there either. The ELO features were
# therefore inert in all three formats, while the pipeline reported success.
#
# The name is declared here, once. Both call sites ask for it rather than
# rebuilding it, which is the defect this repo keeps paying for: two
# declarations of one truth drift, and the drift is silent.

#' Gender Prefix Used by the 3-Way ELO Tables
#'
#' @param gender Character. `"male"`/`"mens"` or `"female"`/`"womens"`.
#' @return `"mens"` or `"womens"`.
#' @keywords internal
three_way_elo_gender_prefix <- function(gender) {
  g <- tolower(trimws(gender[1]))
  if (g %in% c("male", "men", "mens", "m")) return("mens")
  if (g %in% c("female", "women", "womens", "w")) return("womens")
  cli::cli_abort("Unknown gender {.val {gender[1]}} for a 3-way ELO table.")
}

#' Name of a 3-Way ELO Table
#'
#' @param format Character. Anything [normalize_format()] accepts.
#' @param gender Character. See [three_way_elo_gender_prefix()].
#' @return A single table name, e.g. `"mens_t20_3way_elo"`.
#' @keywords internal
three_way_elo_table <- function(format, gender) {
  paste0(three_way_elo_gender_prefix(gender), "_",
         normalize_format(format), "_3way_elo")
}

#' Every 3-Way ELO Table for a Format
#'
#' Both genders, for callers that join a mixed-gender frame. Delivery ids are
#' unique per match, so a `UNION ALL` across the two cannot duplicate a row.
#'
#' @param format Character. Anything [normalize_format()] accepts.
#' @param conn Optional DBI connection. When supplied, tables that do not exist
#'   are dropped from the result rather than producing a query that fails.
#' @return Character vector of table names, possibly empty when `conn` is given.
#' @keywords internal
three_way_elo_tables <- function(format, conn = NULL) {
  tbls <- vapply(c("male", "female"), three_way_elo_table,
                 character(1), format = format, USE.NAMES = FALSE)
  if (!is.null(conn)) tbls <- tbls[vapply(tbls, table_exists, logical(1), conn = conn)]
  tbls
}

#' A SELECT Over Every 3-Way ELO Table for a Format
#'
#' @param format Character. Anything [normalize_format()] accepts.
#' @param columns Character vector of column expressions to select.
#' @param conn DBI connection, used to skip absent tables.
#' @return A single SQL string, or `NULL` when no table exists.
#' @keywords internal
three_way_elo_query <- function(format, columns, conn) {
  tbls <- three_way_elo_tables(format, conn)
  if (length(tbls) == 0) return(NULL)
  cols <- paste(columns, collapse = ",\n    ")
  paste(sprintf("SELECT\n    %s\n  FROM %s", cols, tbls), collapse = "\n  UNION ALL\n  ")
}

# Rebuilding without a window where the table is empty ------------------------
#
# `create_3way_elo_table(..., overwrite = TRUE)` DROPs first and the rebuild
# then computes for hours before its first insert. Interrupt it anywhere in
# between -- Ctrl-C, a crash, a laptop lid -- and the table is left EMPTY with
# nothing to say so. That is how `t20_3way_elo` reached zero rows, and it is
# the same shape as the FORCE_FULL index rebuild and the #45 schema drop.
#
# So the rebuild writes to a staging table and the live one is replaced only
# once the data is complete. An interruption during the computation or the
# insert costs the staging table, which held nothing anyone was reading.

#' Staging Category for a 3-Way ELO Rebuild
#'
#' @param category Character, e.g. `"mens_t20"`.
#' @return The category to build into, e.g. `"mens_t20_staging"`.
#' @keywords internal
three_way_elo_staging_category <- function(category) {
  paste0(tolower(category), "_staging")
}

#' Promote a Completed 3-Way ELO Staging Table Over the Live One
#'
#' Replaces the live table with the staging table in a single transaction, so
#' there is no point at which a reader sees an empty table.
#'
#' @param category Character, e.g. `"mens_t20"`.
#' @param conn A DBI connection with write access.
#' @param min_rows Integer. Refuse to promote fewer rows than this. A staging
#'   table that is empty or tiny means the rebuild failed, and promoting it
#'   would destroy the ratings it was meant to replace.
#' @param min_fraction_of_live Numeric. Also refuse to promote a table holding
#'   less than this share of the CURRENT live table. `min_rows` only compares
#'   against what the run itself expected, so a deliberately limited run --
#'   `MATCH_LIMIT` set for a smoke test -- would pass it and then replace a
#'   full table with a handful of matches. Set to `NULL` to allow a genuine
#'   shrink (a format being rebuilt from a smaller corpus).
#' @return Invisibly, the number of rows promoted.
#' @keywords internal
promote_3way_elo_staging <- function(category, conn, min_rows = 1L,
                                     min_fraction_of_live = 0.9) {
  live <- paste0(tolower(category), "_3way_elo")
  stage <- paste0(three_way_elo_staging_category(category), "_3way_elo")

  if (!table_exists(conn, stage)) {
    cli::cli_abort("No staging table {.val {stage}} to promote.")
  }
  n <- DBI::dbGetQuery(conn, sprintf("SELECT COUNT(*) AS n FROM %s", stage))$n
  if (n < min_rows) {
    cli::cli_abort(c(
      "Staging table {.val {stage}} holds {cli::qty(n)}{n} row{?s}, below the {min_rows} required.",
      "x" = "Refusing to promote -- {.val {live}} would be replaced with nothing.",
      "i" = "The staging table is left in place for inspection."))
  }

  # A limited run is the likelier mistake than a shrinking corpus, so compare
  # against what is already there and make the caller say when a shrink is
  # intended.
  if (!is.null(min_fraction_of_live) && table_exists(conn, live)) {
    n_live <- DBI::dbGetQuery(conn, sprintf("SELECT COUNT(*) AS n FROM %s", live))$n
    if (n_live > 0 && n < n_live * min_fraction_of_live) {
      cli::cli_abort(c(
        "Staging holds {cli::qty(n)}{format(n, big.mark = ',')} row{?s} against {format(n_live, big.mark = ',')} live.",
        "x" = "That is {round(100 * n / n_live, 1)}% of the live table; refusing to promote.",
        "i" = "A MATCH_LIMIT or partial run looks exactly like this.",
        "i" = "Pass {.code min_fraction_of_live = NULL} if the corpus genuinely shrank."))
    }
  }

  # NOT a rename. create_3way_elo_table() declares delivery_id as a PRIMARY
  # KEY, DuckDB backs that with an index, and ALTER TABLE ... RENAME on a table
  # with a dependent index fails: "Cannot alter entry ... because there are
  # entries that depend on it". Recreating the live table from the schema and
  # copying the rows keeps the primary key, which a rename of a schema-less
  # copy would have quietly dropped.
  .in_transaction(conn, function() {
    DBI::dbExecute(conn, sprintf("DROP TABLE IF EXISTS %s", live))
    create_3way_elo_table(tolower(category), conn, overwrite = FALSE)
    DBI::dbExecute(conn, sprintf("INSERT INTO %s SELECT * FROM %s", live, stage))
    DBI::dbExecute(conn, sprintf("DROP TABLE IF EXISTS %s", stage))
  })

  moved <- DBI::dbGetQuery(conn, sprintf("SELECT COUNT(*) AS n FROM %s", live))$n
  if (moved != n) {
    cli::cli_abort(c("Promoted {moved} row{?s} but staging held {n}.",
                     "x" = "The copy did not carry every row."))
  }
  cli::cli_alert_success(
    "Promoted {cli::qty(n)}{format(n, big.mark = ',')} row{?s} into {.val {live}}.")
  invisible(n)
}
