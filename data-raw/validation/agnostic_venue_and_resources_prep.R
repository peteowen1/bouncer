# #59: do a venue-average-score feature or a resource feature earn their place
# in the AGNOSTIC ball-outcome model?
#
# The bar to beat is set by #16: the full model, with player, team and venue
# skills added, beats the agnostic one by T20 0.0% / ODI 0.8% / Test 0.8%, and
# s^2/2*sigma^2 predicts ~0.4%. So the headroom for ANY new state feature is
# about half a percent. Size accordingly and expect a null.
#
# VENUE MUST BE TIME-CAUSAL. A venue average computed over all matches includes
# the one being predicted; at a one-match venue it IS that match's own total.
# That is how the same construction leaked elsewhere (#29, #24, #69). Here it is
# built with time_causal_venue_mean(), matches strictly earlier only.
#
# Venue as a CROSSED EFFECT was already tested and rejected (-1%, D-P22-adjacent);
# this is the different question of venue as a continuous state feature.

suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(data.table); library(xgboost)})

# Scratch location for the cached split and the per-variant scores. Override
# with the first command-line argument; defaults to the session temp dir so the
# script is reproducible on any machine.
OUT <- commandArgs(trailingOnly = TRUE)[1]
if (is.na(OUT) || !nzchar(OUT)) OUT <- file.path(Sys.getenv("TEMP", unset = tempdir()), "agnostic_venue_resources")
dir.create(OUT, showWarnings = FALSE, recursive = TRUE)
FMT <- "t20"; SEED <- 42; FOLDS <- 5; MAX_ROUNDS <- 600; EARLY <- 20
conn <- get_db_connection(read_only = TRUE)
on.exit(dbDisconnect(conn, shutdown = TRUE), add = TRUE)

cli::cli_h1("Loading deliveries")
d <- as.data.table(dbGetQuery(conn, "
  SELECT d.match_id, d.venue, CAST(d.match_date AS DATE) AS match_date,
         d.innings, d.over, d.ball, d.gender, d.match_type,
         (d.total_runs - (d.runs_batter + d.runs_extras)) AS batting_score,
         d.wickets_fallen, d.runs_batter, d.runs_extras, d.is_wicket,
         d.runs_total
  FROM cricsheet.deliveries d
  JOIN cricsheet.matches m ON m.match_id = d.match_id
  WHERE LOWER(d.match_type) IN ('t20','it20') AND d.gender = 'male'
    AND COALESCE(m.balls_per_over, 6) = 6
    AND COALESCE(d.wides, 0) = 0
    AND d.venue IS NOT NULL"))
cli::cli_alert_info("{format(nrow(d), big.mark = ',')} deliveries")

# 7-category outcome, exactly as training defines it
d[, outcome := fcase(is_wicket == 1, 0L, runs_batter == 0, 1L, runs_batter == 1, 2L,
                     runs_batter == 2, 3L, runs_batter == 3, 4L, runs_batter == 4, 5L,
                     runs_batter == 6, 6L, default = NA_integer_)]
d <- d[!is.na(outcome)]
d[, `:=`(over_ball = over + ball/6,
         runs_difference = batting_score,
         overs_left = pmax(0, 20 - (over + ball/6)),
         balls_bowled = over*6 + ball)]

# ---- candidate 1: time-causal venue average first-innings score --------------
cli::cli_h1("Building the time-causal venue average")
inn1 <- d[innings == 1, .(inn1_total = max(batting_score + runs_total)), by = match_id]
vsrc <- unique(d[, .(match_id, venue, match_date)])
vsrc <- merge(vsrc, inn1, by = "match_id", all.x = TRUE)
va <- time_causal_venue_mean(vsrc, "inn1_total", prior_weight = 5)
d <- merge(d, va[, .(match_id, venue_avg_causal = venue_mean)], by = "match_id", all.x = TRUE)
cli::cli_alert_info("venue_avg_causal: mean {round(mean(d$venue_avg_causal),1)}, sd {round(sd(d$venue_avg_causal),1)}")

# ---- candidate 2: resources remaining ---------------------------------------
# The in-match resource surface is a DLS-style "share of scoring capacity left"
# given balls and wickets. If the agnostic model can already deduce it from
# overs_left and wickets_fallen, this adds nothing.
rs_path <- file.path(find_bouncerdata_dir(), "models", sprintf("%s_resource_surface.rds", FMT))
if (!file.exists(rs_path)) cli::cli_abort("No resource surface at {.file {rs_path}}.")
rs <- readRDS(rs_path)
# No tryCatch here. The first version wrapped this call, mistyped the function
# name, and silently reported "resource surface NOT available" -- the arm did
# not run and nothing said so. Let it error.
d[, resources_left := resource_runs(balls_remaining = pmax(0, 120 - balls_bowled),
                                    wickets_in_hand = 10L - wickets_fallen,
                                    surface = rs)]
stopifnot(!anyNA(d$resources_left))
cli::cli_alert_info("resources_left: mean {round(mean(d$resources_left),1)}, sd {round(sd(d$resources_left),1)}")
have_res <- TRUE

# ---- split by match, as training does ---------------------------------------
set.seed(SEED)
um <- unique(d$match_id)
ntr <- floor(0.8 * length(um))
tr <- d[match_id %in% um[1:ntr]]; te <- d[match_id %in% um[(ntr+1):length(um)]]
cli::cli_alert_info("train {format(nrow(tr), big.mark=',')} | test {format(nrow(te), big.mark=',')}")

saveRDS(list(tr = tr, te = te), file.path(OUT, "exp59_data.rds"))
cli::cli_alert_success("prepared data cached")
