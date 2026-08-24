# Audit the bridge players that carry the most weight in the competition
# offsets, for identity merges.
#
# WHY: the competition offset now moves ratings materially -- it dropped ten
# players 400-610 places and moved Karanbir Singh 21 -- and it is estimated
# entirely from players who appear in two competitions. A single player wrongly
# merged from two people IS a fabricated bridge: it claims one person scored X
# here and Y there when two different people did. Asif Ali carries 834 of the
# harmonic weight in International (Developing), more than the next three
# bridgers combined, with 696 balls for Bahrain and 1,041 in the PSL. That is
# what a name collision looks like.
#
# `build_player_id_map()` already excludes 58 ambiguous names, so the machinery
# to get this wrong exists and is running -- the question is what it missed.
#
# Two objective tests, applied to EVERY canonical id (one bad merge predicts
# siblings, so the whole set gets audited, then ranked by bridge weight):
#
#   A. more than one distinct INTERNATIONAL team. Genuine switchers exist
#      (Morgan, Rankin, Ronchi), so this flags rather than convicts.
#   B. two DIFFERENT matches on the same date at different venues. A person
#      cannot be in two places at once; double-headers at one venue can, so
#      the venue check is what makes this decisive rather than suggestive.
suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
conn <- dbConnect(duckdb::duckdb(), dbdir = file.path(find_bouncerdata_dir(), "bouncer.duckdb"),
                  read_only = TRUE)
on.exit(dbDisconnect(conn, shutdown = TRUE), add = TRUE)
id_map <- build_player_id_map(conn)

app <- as.data.table(dbGetQuery(conn, "
  SELECT DISTINCT d.batter_id AS player_id, d.match_id, m.match_date,
         d.batting_team AS team, m.venue, m.team_type, m.match_type
  FROM cricsheet.deliveries d JOIN cricsheet.matches m ON m.match_id = d.match_id
  WHERE m.gender = 'male' AND LOWER(d.match_type) IN ('t20','it20')"))
canonicalise_player_ids(app, id_map)
nm <- unique(as.data.table(dbGetQuery(conn,
  "SELECT player_id, player_name FROM cricsheet.players")), by = "player_id")
canonicalise_player_ids(nm, id_map)
nm <- unique(nm, by = "player_id")

# --- test A: multiple international teams ------------------------------------
intl <- app[team_type == "international",
            .(n_teams = uniqueN(team), teams = paste(sort(unique(team)), collapse = " / ")),
            by = player_id]

# --- test B: two different matches, same date, different venues --------------
setorder(app, player_id, match_date)
clash <- app[, {
  d <- .SD[, .(nm_ = uniqueN(match_id), nv = uniqueN(venue)), by = match_date]
  bad <- d[nm_ > 1 & nv > 1]
  .(n_clash = nrow(bad), first_clash = if (nrow(bad)) as.character(bad$match_date[1]) else NA_character_)
}, by = player_id]

# --- bridge weight, exactly as fit_competition_offsets() computes it ---------
bal <- as.data.table(dbGetQuery(conn, sprintf("
  SELECT r.batter_id AS player_id, %s AS comp, COUNT(*) balls
  FROM main.cricsheet_ball_raa r JOIN cricsheet.matches m ON m.match_id = r.match_id
  WHERE r.format='T20' AND r.gender='male' GROUP BY 1,2", .competition_sql("t20"))))
canonicalise_player_ids(bal, id_map)
bal <- bal[, .(balls = sum(balls)), by = .(player_id, comp)]
REF <- COMPETITION_REFERENCE_T20
rb <- bal[comp %in% REF, .(r_balls = sum(balls)), by = player_id]
br <- merge(bal[!comp %in% REF], rb, by = "player_id")
br[, w := 2 * balls * r_balls / (balls + r_balls)]
wt <- br[, .(bridge_w = sum(w), n_comps = uniqueN(comp)), by = player_id]

au <- Reduce(function(x, y) merge(x, y, by = "player_id", all.x = TRUE),
             list(wt, intl, clash, nm))
au[is.na(n_teams), n_teams := 0L][is.na(n_clash), n_clash := 0L]
au[, suspect := n_teams > 1 | n_clash > 0]
setorder(au, -bridge_w)

cat(sprintf("=== T20 men: identity audit of %d canonical ids that bridge a competition ===\n",
            nrow(au)))
cat(sprintf("Flagged by at least one test: %d (%.1f%%)\n",
            au[suspect == TRUE, .N], 100 * au[suspect == TRUE, .N] / nrow(au)))
cat(sprintf("  multiple international teams : %d\n", au[n_teams > 1, .N]))
cat(sprintf("  same-date, different venues  : %d\n", au[n_clash > 0, .N]))
cat(sprintf("Share of ALL bridge weight sitting on a flagged id: %.1f%%\n\n",
            100 * au[suspect == TRUE, sum(bridge_w)] / au[, sum(bridge_w)]))

cat("--- top 20 bridges by weight, flagged or not ---\n")
cat(sprintf("%-22s %9s %6s %6s %7s  %s\n",
            "player", "bridge w", "comps", "teams", "clashes", "international teams"))
for (i in 1:20) with(au[i], cat(sprintf("%-22s %9.0f %6d %6d %7d  %s\n",
  substr(ifelse(is.na(player_name), "?", player_name), 1, 22), bridge_w, n_comps,
  n_teams, n_clash, substr(ifelse(is.na(teams), "-", teams), 1, 46))))

flagged <- au[suspect == TRUE][order(-bridge_w)]
cat(sprintf("\n--- top 20 FLAGGED bridges by weight (n = %d flagged) ---\n", nrow(flagged)))
if (nrow(flagged)) {
  cat(sprintf("%-22s %9s %6s %7s  %s\n", "player", "bridge w", "teams", "clashes", "international teams"))
  for (i in 1:min(20, nrow(flagged))) with(flagged[i], cat(sprintf(
    "%-22s %9.0f %6d %7d  %s\n",
    substr(ifelse(is.na(player_name), "?", player_name), 1, 22), bridge_w,
    n_teams, n_clash, substr(ifelse(is.na(teams), "-", teams), 1, 46))))
}

cat("\n--- Asif Ali, the case that prompted this ---\n")
aa <- au[grepl("^Asif Ali", player_name)]
if (nrow(aa)) {
  for (i in 1:nrow(aa)) with(aa[i], cat(sprintf(
    "  bridge weight %.0f, %d competitions, %d international team(s): %s, %d date clashes\n",
    bridge_w, n_comps, n_teams, ifelse(is.na(teams), "-", teams), n_clash)))
  print(bal[player_id %in% aa$player_id][order(-balls)][1:8])
} else cat("  not found\n")

# --- test C: do the two international careers INTERLEAVE? --------------------
#
# Test A over-flags badly. ICC World XI is a real composite side, and genuine
# eligibility switchers exist (Chapman HK/NZ, Ross Taylor NZ/Samoa, Tim David
# AUS/SGP, Wiese NAM/RSA, van der Merwe NED/RSA). What separates a switcher from
# a merge is TIME: a switcher's two careers are sequential, because eligibility
# rules impose a stand-down. A merged pair of people plays for both teams in the
# same period. Overlapping date ranges is therefore close to decisive.
cat("\n=== test C: overlapping international careers (ICC World XI excluded) ===\n")
iv <- app[team_type == "international" & team != "ICC World XI",
          .(first = min(match_date), last = max(match_date), n = uniqueN(match_id)),
          by = .(player_id, team)]
multi <- iv[, .N, by = player_id][N > 1, player_id]
ov <- iv[player_id %in% multi][order(player_id, first)][, {
  o <- FALSE
  if (.N > 1) for (i in 2:.N) if (first[i] <= last[i - 1L]) o <- TRUE
  .(teams = paste(sprintf("%s (%s..%s, %d)", team, first, last, n), collapse = "  |  "),
    overlap = o)
}, by = player_id]
ov <- merge(ov, nm, by = "player_id", all.x = TRUE)
ov <- merge(ov, wt[, .(player_id, bridge_w)], by = "player_id", all.x = TRUE)
setorder(ov, -overlap, -bridge_w)
cat(sprintf("%d players hold two non-World-XI international caps; %d OVERLAP in time.\n\n",
            nrow(ov), ov[overlap == TRUE, .N]))
for (i in seq_len(nrow(ov))) with(ov[i], cat(sprintf(
  "%-3s %-20s w %6.0f  %s\n", if (overlap) "!!" else "ok",
  substr(ifelse(is.na(player_name), "?", player_name), 1, 20),
  ifelse(is.na(bridge_w), 0, bridge_w), teams)))

cat("\n=== test B detail: same date, two matches, different venues ===\n")
cl <- merge(clash[n_clash > 0], nm, by = "player_id", all.x = TRUE)
cl <- merge(cl, wt[, .(player_id, bridge_w)], by = "player_id", all.x = TRUE)
setorder(cl, -bridge_w)
for (i in seq_len(nrow(cl))) with(cl[i], cat(sprintf("  %-22s w %6.0f  %d clash(es), first %s\n",
  substr(ifelse(is.na(player_name), "?", player_name), 1, 22),
  ifelse(is.na(bridge_w), 0, bridge_w), n_clash, first_clash)))
