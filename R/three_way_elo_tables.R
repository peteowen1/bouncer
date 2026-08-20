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
