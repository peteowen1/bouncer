# Assembling Simulation Inputs
#
# simulate_match_ballbyball() (R/simulation.R) needs a fully-formed set of
# arguments: a batter skill list and a bowler skill list per team, one team
# skill object per team, and a venue skill object. The pieces that produce
# each of those already exist -- get_player()/direct player-skill lookups,
# get_team_skill() (R/team_skill_index.R), get_venue_skill()
# (R/venue_skill_index.R) -- but nothing turned "team A vs team B at venue V"
# into the shapes the simulator reads. This file is that glue.
#
# TWO SOURCES OF TRUTH FOR "WHO PLAYED":
#   - Historical match (match_id given): the real XI comes from
#     main.match_squads, built from cricsheet's own info.players (see
#     data-raw/data-acquisition/build_match_squads.R). Preferred whenever it
#     exists -- appearance-based squads leak the result into the feature
#     (score_team_rating.R), a named XI cannot.
#   - Hypothetical fixture (no match_id): there is no squad to read, so the
#     caller supplies player names directly.
#
# A NAME WITHOUT A SKILL ROW GETS LEAGUE-AVERAGE DEFAULTS -- THAT IS
# UNAVOIDABLE (the simulator needs *some* number for every player). What is
# avoidable is doing that silently. Every roster returned here carries
# n_*_resolved counts and an unresolved_players vector, and a team that comes
# back with a resolution rate of zero triggers a loud cli_warn, not a quiet
# fallback -- see CLAUDE.md's "characteristic bug" note: a neutral fill that
# reads as a real value.


#' Batch-Resolve Player Skills From the Format Skill Table
#'
#' Looks up the latest batting-role and bowling-role skill row for each
#' player id, in two separate queries. This is deliberately NOT the same
#' query `get_player()` runs (`WHERE batter_id = ? OR bowler_id = ?` pulling
#' all four skill columns from whichever row matched): that table is keyed
#' per delivery, one row per ball, and a row where a player appears as
#' `batter_id` carries the *opposing bowler's* `bowler_economy_index` /
#' `bowler_strike_rate` in the same row, not the player's own bowling skill.
#' Pulling all four columns off one row silently mixes the two players. This
#' function only ever reads `batter_*` columns from a row where the player
#' was the batter, and `bowler_*` columns from a row where the player was the
#' bowler, so the two roles never cross-contaminate.
#'
#' @param player_ids Character vector. Player ids to resolve (may contain
#'   `NA`, which is dropped before querying).
#' @param format Character. Normalized format ("t20", "odi", "test").
#' @param conn DBI connection.
#'
#' @return data.table keyed by `player_id` with `batter_scoring_index`,
#'   `batter_survival_rate`, `bowler_economy_index`, `bowler_strike_rate`,
#'   `resolved_batter`, `resolved_bowler` (logical). One row per distinct
#'   non-NA input id; ids never seen in either role still get a row with both
#'   `resolved_*` FALSE, so the caller can join without losing anyone.
#' @keywords internal
batch_resolve_player_skills <- function(player_ids, format, conn) {
  ids <- unique(player_ids[!is.na(player_ids)])
  empty <- data.table::data.table(
    player_id = character(0),
    batter_scoring_index = double(0), batter_survival_rate = double(0),
    bowler_economy_index = double(0), bowler_strike_rate = double(0),
    resolved_batter = logical(0), resolved_bowler = logical(0)
  )
  if (length(ids) == 0) return(empty)

  skill_table <- paste0(format, "_player_skill")
  validate_sql_identifier(skill_table, context = "batch_resolve_player_skills")
  if (!table_exists(conn, skill_table)) {
    out <- data.table::data.table(player_id = ids)
    out[, `:=`(batter_scoring_index = NA_real_, batter_survival_rate = NA_real_,
               bowler_economy_index = NA_real_, bowler_strike_rate = NA_real_,
               resolved_batter = FALSE, resolved_bowler = FALSE)]
    return(out)
  }

  ids_esc <- paste(escape_sql_quotes(ids), collapse = "','")

  bat <- data.table::as.data.table(DBI::dbGetQuery(conn, sprintf("
    SELECT player_id, batter_scoring_index, batter_survival_rate FROM (
      SELECT batter_id AS player_id, batter_scoring_index, batter_survival_rate,
             ROW_NUMBER() OVER (PARTITION BY batter_id
                                 ORDER BY match_date DESC, delivery_id DESC) AS rn
      FROM %s WHERE batter_id IN ('%s')
    ) WHERE rn = 1
  ", skill_table, ids_esc)))

  bowl <- data.table::as.data.table(DBI::dbGetQuery(conn, sprintf("
    SELECT player_id, bowler_economy_index, bowler_strike_rate FROM (
      SELECT bowler_id AS player_id, bowler_economy_index, bowler_strike_rate,
             ROW_NUMBER() OVER (PARTITION BY bowler_id
                                 ORDER BY match_date DESC, delivery_id DESC) AS rn
      FROM %s WHERE bowler_id IN ('%s')
    ) WHERE rn = 1
  ", skill_table, ids_esc)))

  out <- data.table::data.table(player_id = ids)
  out <- merge(out, bat, by = "player_id", all.x = TRUE)
  out <- merge(out, bowl, by = "player_id", all.x = TRUE)
  out[, resolved_batter := !is.na(batter_scoring_index)]
  out[, resolved_bowler := !is.na(bowler_economy_index)]
  out[]
}


#' Look Up a Squad From `main.match_squads`
#'
#' @param match_id Character. Match id.
#' @param team Character. Team name exactly as it appears in
#'   `cricsheet.matches` / `main.match_squads` (e.g. "India").
#' @param conn DBI connection.
#' @return data.table of `player_id`, `player_name` (zero rows if the match
#'   has no recorded squad for this team).
#' @keywords internal
lookup_match_squad <- function(match_id, team, conn) {
  if (!table_exists(conn, "main.match_squads")) {
    return(data.table::data.table(player_id = character(0), player_name = character(0)))
  }
  data.table::as.data.table(DBI::dbGetQuery(conn,
    "SELECT player_id, player_name FROM main.match_squads
     WHERE match_id = ? AND team = ?", params = list(match_id, team)))
}


#' Resolve Caller-Supplied Player Names to Ids
#'
#' Used for the hypothetical-fixture path, where there is no squad row to
#' read a player_id from. Deliberately simpler than [find_player()]: this
#' only needs an id, not a career-ball-volume ranking, and skipping that
#' avoids joining the full deliveries table per player for what is usually a
#' handful of names. Falls through exact match -> case-insensitive exact ->
#' substring, same tiering rationale as `find_player()` / `get_player()`
#' (an ambiguous or absent match must not silently pick a false positive).
#'
#' @param names Character vector of player names.
#' @param conn DBI connection.
#' @return data.table of `player_name` (the input, unchanged) and `player_id`
#'   (`NA` if nothing matched).
#' @keywords internal
resolve_named_players <- function(names, conn, format = NULL, gender = NULL) {
  # Delegates to find_player() rather than re-picking hit[1] itself -- an
  # unordered ambiguous match silently resolving to whichever row a GROUP BY
  # happened to return first is exactly the class of bug find_player() was
  # written to make loud (see R/find_player.R): it breaks ties by career ball
  # volume and warns whenever more than one candidate matches.
  resolve_one <- function(nm) {
    hit <- find_player(nm, format = format, gender = gender, conn = conn, quiet = FALSE)
    if (!nrow(hit)) return(NA_character_)
    hit$player_id[1]
  }
  data.table::data.table(player_name = names,
                         player_id = vapply(names, resolve_one, character(1)))
}


#' Build One Team's Batter/Bowler Skill Lists
#'
#' The shared roster-building step behind [build_match_simulation_inputs()]:
#' takes a set of (player_id, player_name) pairs -- however they were
#' sourced -- resolves skills for all of them in two batched queries, and
#' returns the shapes [simulate_match_ballbyball()] reads.
#'
#' Batters are returned in roster order (registry/squad order -- neither
#' `main.match_squads` nor a caller's name vector carries a real batting
#' order, so this is the best available and callers who want a specific
#' order should reorder before passing it on). Bowlers are the SAME roster,
#' reordered so players with a resolved bowling-skill row sort first: there
#' is no role column anywhere upstream (`main.match_squads` lists an XI, not
#' batters vs. bowlers), so "has bowled a ball in this format before" is the
#' only real signal available for who should get the overs.
#'
#' @param roster data.table with `player_id`, `player_name` (one row per
#'   squad/roster member; `player_id` may be `NA` for an unresolved name).
#' @param format Character. Normalized format.
#' @param conn DBI connection.
#' @param id_map Optional canonical-id map from [build_player_id_map()].
#'   `main.match_squads` ids are name-keyed for ~4-5% of rows (bouncerverse#74)
#'   and will silently miss the skill-table join without this; pass a
#'   pre-built map when assembling many fixtures (a season/tournament) to
#'   amortise its one expensive full-table scan, and skip it for a single
#'   ad hoc match.
#'
#' @return List with `batters`, `bowlers` (each a list of per-player skill
#'   lists, length `nrow(roster)`), `n_players`, `n_batters_resolved`,
#'   `n_bowlers_resolved`, `unresolved_players` (names with neither role
#'   resolved).
#' @keywords internal
build_team_roster_skills <- function(roster, format, conn, id_map = NULL) {
  start_vals <- get_skill_start_values(format)
  n <- nrow(roster)

  lookup_ids <- roster$player_id
  if (!is.null(id_map) && nrow(id_map) > 0) {
    m <- stats::setNames(id_map$canonical_id, id_map$player_id)
    hit <- m[lookup_ids]
    lookup_ids <- ifelse(is.na(hit), lookup_ids, unname(hit))
  }

  skills <- batch_resolve_player_skills(lookup_ids, format, conn)
  # Index-match rather than merge(): merge()/join do not guarantee the
  # original roster order survives, and roster order IS the batting order
  # this function promises to preserve.
  idx <- match(lookup_ids, skills$player_id)
  roster <- data.table::copy(roster)
  roster[, `:=`(
    batter_scoring_index = skills$batter_scoring_index[idx],
    batter_survival_rate = skills$batter_survival_rate[idx],
    bowler_economy_index = skills$bowler_economy_index[idx],
    bowler_strike_rate = skills$bowler_strike_rate[idx],
    resolved_batter = !is.na(idx) & skills$resolved_batter[idx],
    resolved_bowler = !is.na(idx) & skills$resolved_bowler[idx]
  )]

  batters <- lapply(seq_len(n), function(i) {
    list(
      batter_scoring_index = if (isTRUE(roster$resolved_batter[i])) roster$batter_scoring_index[i] else start_vals$scoring_index,
      batter_survival_rate = if (isTRUE(roster$resolved_batter[i])) roster$batter_survival_rate[i] else start_vals$survival_rate,
      batter_balls_faced = 0L
    )
  })
  bowlers_order <- order(!roster$resolved_bowler) # resolved bowlers first, stable within each group
  bowlers <- lapply(bowlers_order, function(i) {
    list(
      bowler_economy_index = if (isTRUE(roster$resolved_bowler[i])) roster$bowler_economy_index[i] else start_vals$economy_index,
      bowler_strike_rate = if (isTRUE(roster$resolved_bowler[i])) roster$bowler_strike_rate[i] else start_vals$strike_rate,
      bowler_balls_bowled = 0L
    )
  })

  unresolved <- roster$player_name[!roster$resolved_batter & !roster$resolved_bowler]

  list(
    batters = batters,
    bowlers = bowlers,
    n_players = n,
    n_batters_resolved = sum(roster$resolved_batter),
    n_bowlers_resolved = sum(roster$resolved_bowler),
    unresolved_players = unresolved
  )
}


#' Build One Team's Skill Object (Batting and Bowling Roles)
#'
#' `get_team_skill()` returns a role-specific list, always named generically
#' (`runs_skill`/`wicket_skill`) regardless of which role was requested --
#' the batting-role call answers "how much better does this team score than
#' context-expected", the bowling-role call answers "how much better does it
#' restrict runs/take wickets", and those are two different underlying
#' numbers reusing the same field names. A team has both.
#'
#' @param team_id Character. Composite team id from [make_team_id()].
#' @param format Character. Normalized format.
#' @param conn DBI connection.
#' @return List with `batting` and `bowling`, each `list(runs_skill=,
#'   wicket_skill=, resolved=)`. `resolved = FALSE` means the team was never
#'   seen in that role in this format and the pair defaults to neutral (0, 0).
#' @keywords internal
build_team_skill_pair <- function(team_id, format, conn) {
  as_role <- function(role) {
    s <- get_team_skill(team_id, role = role, format = format, conn = conn)
    if (is.null(s)) {
      list(runs_skill = 0, wicket_skill = 0, resolved = FALSE)
    } else {
      list(runs_skill = s$runs_skill, wicket_skill = s$wicket_skill, resolved = TRUE)
    }
  }
  list(batting = as_role("batting"), bowling = as_role("bowling"))
}


#' Build One Team's Roster + Team-Skill Bundle
#'
#' @keywords internal
build_team_simulation_inputs <- function(team, format, conn, gender, team_type,
                                          match_id = NULL, players = NULL,
                                          id_map = NULL) {
  source <- NA_character_
  roster <- data.table::data.table(player_id = character(0), player_name = character(0))

  if (!is.null(match_id)) {
    roster <- lookup_match_squad(match_id, team, conn)
    if (nrow(roster) > 0) source <- "squad"
  }
  if (nrow(roster) == 0) {
    if (is.null(players) || length(players) == 0) {
      cli::cli_abort(c(
        "No squad found for {.val {team}} in match {.val {match_id}}, and no {.arg players} supplied.",
        "i" = "A hypothetical fixture (no match_id) must supply player names explicitly."
      ))
    }
    roster <- resolve_named_players(players, conn, format = format, gender = gender)
    source <- "caller_supplied"
  }

  roster_skills <- build_team_roster_skills(roster, format, conn, id_map = id_map)

  if (roster_skills$n_batters_resolved == 0 && roster_skills$n_bowlers_resolved == 0) {
    cli::cli_warn(c(
      "!" = "{.val {team}}'s entire roster ({roster_skills$n_players} players) resolved to LEAGUE-AVERAGE defaults.",
      "i" = "Zero of {roster_skills$n_players} players had a {format} skill row -- check names/ids before trusting this simulation."
    ))
  } else if (roster_skills$n_batters_resolved < roster_skills$n_players / 2) {
    cli::cli_alert_warning(
      "{.val {team}}: only {roster_skills$n_batters_resolved}/{roster_skills$n_players} players resolved a batting skill row; the rest are league-average.")
  }

  team_id <- make_team_id(team, gender, format, team_type)
  team_skills <- build_team_skill_pair(team_id, format, conn)
  if (!team_skills$batting$resolved && !team_skills$bowling$resolved) {
    cli::cli_alert_warning(
      "No team-skill history for {.val {team}} ({.val {team_id}}); using neutral (0, 0) team skill.")
  }

  c(list(team = team, team_id = team_id, source = source, team_skills = team_skills),
    roster_skills)
}


#' Build Venue Skills for the Simulator
#'
#' Wraps [get_venue_skill()] and always returns the `venue_`-prefixed field
#' names [simulate_delivery()] reads as its primary spelling (that function
#' also accepts the bare `run_rate`/`wicket_rate`/... spelling as a fallback,
#' but relying on the fallback is exactly the kind of silent-neutralisation
#' risk this file exists to avoid -- see the file header).
#'
#' @param venue Character. Venue name.
#' @param format Character. Normalized format.
#' @param conn DBI connection.
#' @return List with `venue_run_rate`, `venue_wicket_rate`,
#'   `venue_boundary_rate`, `venue_dot_rate`, `venue_resolved` (logical).
#' @keywords internal
build_venue_simulation_skills <- function(venue, format, conn) {
  vs <- get_venue_skill(venue, format = format, conn = conn)
  if (is.null(vs)) {
    cli::cli_alert_warning("No venue skill history for {.val {venue}} ({format}); using neutral defaults.")
    start_vals <- get_venue_start_values(format)
    return(list(
      venue_run_rate = start_vals$run_rate, venue_wicket_rate = start_vals$wicket_rate,
      venue_boundary_rate = start_vals$boundary_rate, venue_dot_rate = start_vals$dot_rate,
      venue_resolved = FALSE
    ))
  }
  list(
    venue_run_rate = vs$run_rate, venue_wicket_rate = vs$wicket_rate,
    venue_boundary_rate = vs$boundary_rate, venue_dot_rate = vs$dot_rate,
    venue_resolved = TRUE
  )
}


#' Assemble Match Simulation Inputs
#'
#' The missing glue between "team A vs team B at venue V" and the argument
#' shapes [simulate_match_ballbyball()] takes: a batter skill list and a
#' bowler skill list per team, a team-skill object per team, and a venue
#' skill object. See the file header for the design rationale.
#'
#' Two ways to call this:
#'   - **Historical match**: pass `match_id`. `team1`/`team2`/`venue`/`format`/
#'     `gender`/`team_type` are read from `cricsheet.matches` and the real XI
#'     from `main.match_squads`; any of those arguments passed explicitly are
#'     ignored in favour of the match record (a historical match has ground
#'     truth; use it).
#'   - **Hypothetical fixture**: leave `match_id` NULL and supply `team1`,
#'     `team2`, `venue`, `format`, `team1_players`, `team2_players` yourself.
#'     There is no squad to read, so player names are required.
#'
#' @param conn DBI connection.
#' @param match_id Character. If supplied, the historical path is used and
#'   `team1`/`team2`/`venue`/`format`/`gender`/`team_type` come from
#'   `cricsheet.matches` instead of the arguments below.
#' @param team1,team2 Character. Team names as they appear in
#'   `cricsheet.matches` (e.g. "India"), not a composite team_id. Required
#'   for a hypothetical fixture; ignored (a message is printed if they
#'   disagree with the match record) when `match_id` resolves.
#' @param venue Character. Venue name. Same rule as `team1`/`team2`.
#' @param format Character. "t20", "odi", or "test". Same rule.
#' @param gender Character. "male" or "female". Default "male". Same rule.
#' @param team_type Character. "international" or "club" -- feeds
#'   [make_team_id()] for the team-skill lookup. Default "international".
#'   Same rule.
#' @param team1_players,team2_players Character vectors of player names.
#'   Required when `match_id` is NULL or has no recorded squad. Ignored when
#'   a squad is found.
#' @param id_map Optional canonical-id map from [build_player_id_map()],
#'   forwarded to [build_team_roster_skills()] to fix the ~4-5% of
#'   `main.match_squads` rows that are name-keyed rather than
#'   registry-id-keyed (bouncerverse#74). Building this map is one expensive
#'   full-table scan; build it once and pass it in when assembling many
#'   fixtures (a season/tournament), and omit it for a single match.
#'
#' @return List with `match_id`, `format`, `gender`, `team_type`, `team1`,
#'   `team2` (each from [build_team_simulation_inputs()]: `team`, `team_id`,
#'   `source` ("squad" or "caller_supplied"), `batters`, `bowlers`,
#'   `n_players`, `n_batters_resolved`, `n_bowlers_resolved`,
#'   `unresolved_players`, `team_skills` (`$batting`/`$bowling`, each
#'   `runs_skill`/`wicket_skill`/`resolved`)), and `venue_skills`.
#'
#' @section ELO features are deliberately NOT wired in:
#' `simulate_delivery()` currently omits `elo_run_diff`/`elo_wicket_diff`/
#' `elo_venue_run`, and `prepare_full_features()` defaults them to 0 when
#' absent. Whether this assembler should start supplying real ELO diffs is
#' an open call on bouncerverse#66 (the full model was retrained on real ELO
#' coverage per bouncerverse#65, but is not yet wired into ANY consumer --
#' see bouncer/CLAUDE.md's ELO section) -- not decided here, so this
#' function does not touch it either way.
#'
#' @seealso [simulate_match_ballbyball()] for what consumes this output.
#' @export
build_match_simulation_inputs <- function(conn, match_id = NULL,
                                           team1 = NULL, team2 = NULL, venue = NULL,
                                           format = NULL, gender = "male",
                                           team_type = "international",
                                           team1_players = NULL, team2_players = NULL,
                                           id_map = NULL) {
  if (!is.null(match_id)) {
    mi <- DBI::dbGetQuery(conn,
      "SELECT team1, team2, venue, match_type, gender, team_type
       FROM cricsheet.matches WHERE match_id = ?", params = list(match_id))
    if (nrow(mi) == 0) {
      cli::cli_abort("match_id {.val {match_id}} not found in cricsheet.matches.")
    }
    if ((!is.null(team1) && team1 != mi$team1[1]) ||
        (!is.null(team2) && team2 != mi$team2[1])) {
      cli::cli_alert_info("Overriding supplied team1/team2 with the {.val {match_id}} match record.")
    }
    team1 <- mi$team1[1]; team2 <- mi$team2[1]; venue <- mi$venue[1]
    format <- normalize_format(mi$match_type[1])
    # `%||%` only catches NULL, not NA -- dbGetQuery returns NA for a SQL
    # NULL, so it would silently fail to fall back here.
    if (!is.na(mi$gender[1])) gender <- mi$gender[1]
    if (!is.na(mi$team_type[1])) team_type <- mi$team_type[1]
  } else {
    if (is.null(team1) || is.null(team2) || is.null(venue) || is.null(format)) {
      cli::cli_abort(c(
        "A hypothetical fixture needs {.arg team1}, {.arg team2}, {.arg venue} and {.arg format}.",
        "i" = "Pass {.arg match_id} instead for a historical fixture."
      ))
    }
    format <- normalize_format(format)
  }

  t1 <- build_team_simulation_inputs(team1, format, conn, gender, team_type,
                                     match_id = match_id, players = team1_players,
                                     id_map = id_map)
  t2 <- build_team_simulation_inputs(team2, format, conn, gender, team_type,
                                     match_id = match_id, players = team2_players,
                                     id_map = id_map)
  venue_skills <- build_venue_simulation_skills(venue, format, conn)

  list(
    match_id = match_id, format = format, gender = gender, team_type = team_type,
    venue = venue, team1 = t1, team2 = t2, venue_skills = venue_skills
  )
}
