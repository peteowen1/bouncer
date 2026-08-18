# Reusable player context for every table: country, modal competition, ICC rank.
#
# Country is the player's modal INTERNATIONAL team in cricsheet (it20/odi/test),
# which is the only country signal in the database itself -- the crosswalk with
# a country column lives in a bouncerdata release, not locally.
player_context <- function(conn, fmt_types = "'t20','it20'") {
  ctry <- data.table::as.data.table(DBI::dbGetQuery(conn, "
    SELECT player_id, team AS country FROM (
      SELECT d.batter_id AS player_id, d.batting_team AS team, COUNT(*) AS n,
             ROW_NUMBER() OVER (PARTITION BY d.batter_id ORDER BY COUNT(*) DESC) AS rn
      FROM cricsheet.deliveries d
      JOIN cricsheet.matches m ON m.match_id = d.match_id
      WHERE LOWER(d.match_type) IN ('it20','odi','test') AND m.team_type = 'international'
      GROUP BY 1,2) WHERE rn = 1"))
  icc <- data.table::fread("C:/dev/bouncerverse/docs/reference/icc-rankings-2026-08.csv")
  list(country = ctry, icc = icc)
}

# Attach country + ICC rank to a rating table that already has player_name.
with_context <- function(rat, ctx, icc_format = "t20i", icc_discipline = "batting") {
  r <- data.table::copy(data.table::as.data.table(rat))
  r <- merge(r, ctx$country, by = "player_id", all.x = TRUE)
  ic <- ctx$icc[format == icc_format & discipline == icc_discipline,
                .(icc_name = player_name, icc = rank)]
  # ICC uses full names, cricsheet uses initials -- match on surname + a first
  # initial that is consistent with the full first name. Deliberately
  # conservative: an ambiguous surname yields NA rather than a guess, because
  # a wrong ICC rank in a table is worse than a blank one.
  sur <- function(x) tolower(sub(".* ", "", trimws(x)))
  ini <- function(x) tolower(substr(trimws(x), 1, 1))
  ic[, `:=`(s = sur(icc_name), i = ini(icc_name))]
  r[, `:=`(s = sur(player_name), i = tolower(substr(trimws(player_name), 1, 1)))]
  m <- merge(r, ic, by = "s", allow.cartesian = TRUE)
  m <- m[i.x == i.y | i.x == i.y | grepl(paste0("^", i.y), i.x)]
  m <- m[, .SD[1L], by = player_id][, .(player_id, icc)]
  amb <- ic[, .N, by = s][N > 1, s]
  m <- m[!player_id %in% r[s %in% amb, player_id]]
  r <- merge(r, m, by = "player_id", all.x = TRUE)
  r[, c("s","i") := NULL][]
}

fmt_tbl <- function(d, cols, n = 10) {
  d <- data.table::copy(d)[seq_len(min(n, nrow(d)))]
  d[, country := ifelse(is.na(country), "-", country)]
  d[, icc := ifelse(is.na(icc), "-", as.character(icc))]
  d[, .SD, .SDcols = cols]
}
