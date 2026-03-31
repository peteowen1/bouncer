# Train PSR Model (glmnet) ----
#
# Trains elastic-net models to predict match margin from team-aggregated
# stat ratings. Produces three coefficient sets:
#   - psr_coefficients.csv: margin ~ all ratings (authoritative total)
#   - batv_coefficients.csv: team_runs ~ batting_ratings + opp_bowling_ratings
#   - bowlv_coefficients.csv: team_runs_conceded ~ bowling_ratings + opp_batting_ratings
#
# Adapted from torpverse/torp/data-raw/06-stat-ratings/06_train_psr_model.R

library(cli)
library(data.table)
devtools::load_all()

if (!requireNamespace("glmnet", quietly = TRUE)) {
  stop("Package 'glmnet' required. Install with: install.packages('glmnet')")
}

# ============================================================================
# Configuration
# ============================================================================

FORMAT <- "t20"  # Train on T20 (most data)
MIN_MATCHES_PER_TEAM <- 5

# ============================================================================
# Load data
# ============================================================================

cli::cli_h1("Training PSR model for {toupper(FORMAT)}")

conn <- get_db_connection(read_only = TRUE)

# Load stat ratings directly from conn (avoid shutdown issue)
ratings <- data.table::as.data.table(DBI::dbGetQuery(conn,
  sprintf("SELECT * FROM %s_stat_ratings ORDER BY wt_matches DESC", FORMAT)))
cat(sprintf("Loaded %d player ratings\n", nrow(ratings)))

# Load player game data directly from conn
pgd <- data.table::as.data.table(DBI::dbGetQuery(conn,
  sprintf("SELECT * FROM %s_player_game_data", FORMAT)))

# Get match results from Cricinfo
match_results <- DBI::dbGetQuery(conn, sprintf("
  SELECT
    m.match_id,
    m.team1_name AS team1,
    m.team2_name AS team2,
    m.winner_team_id,
    m.team1_id,
    m.team2_id,
    m.start_date
  FROM cricinfo.matches m
  WHERE %s
    AND m.winner_team_id IS NOT NULL
", cricinfo_format_sql("m.format", FORMAT)))
match_results <- as.data.table(match_results)

# Compute margin from player game data (team1 runs - team2 runs)
# First, determine who batted in each match and their totals
team_scores <- pgd[, .(
  team_runs = sum(batting_runs, na.rm = TRUE)
), by = .(match_id)]

# Get per-team batting totals
# We need to identify which team each player belongs to from innings data
team_batting <- DBI::dbGetQuery(conn, sprintf("
  SELECT
    b.match_id,
    b.innings_number,
    SUM(b.total_runs) AS innings_total
  FROM cricinfo.balls b
  JOIN cricinfo.matches m ON b.match_id = m.match_id
  WHERE %s
  GROUP BY b.match_id, b.innings_number
  ORDER BY b.match_id, b.innings_number
", cricinfo_format_sql("m.format", FORMAT)))
team_batting <- as.data.table(team_batting)

# For T20/ODI: team1 = innings 1, team2 = innings 2 (simplified)
# Margin = innings_1_total - innings_2_total (from batting first team perspective)
margins <- team_batting[, .(
  innings1_total = sum(innings_total[innings_number == 1]),
  innings2_total = sum(innings_total[innings_number == 2]),
  margin = sum(innings_total[innings_number == 1]) - sum(innings_total[innings_number == 2])
), by = match_id]

cat(sprintf("Matches with margins: %d\n", nrow(margins)))
cat(sprintf("Mean margin: %.1f, SD: %.1f\n", mean(margins$margin), sd(margins$margin)))

DBI::dbDisconnect(conn, shutdown = TRUE)
on.exit(NULL)

# ============================================================================
# Build feature matrix: team-aggregated stat ratings per match (team-side)
# ============================================================================

cli::cli_h2("Building feature matrix (team-side separation)")

# Get rating columns
rating_cols <- grep("_rating$", names(ratings), value = TRUE)
rating_cols <- setdiff(rating_cols, grep("_lower|_upper", names(ratings), value = TRUE))
cat(sprintf("Using %d rating features per team-side\n", length(rating_cols)))

# Join ratings to player game data (which has team assignments)
pgd_rated <- merge(pgd, ratings[, c("player_id", rating_cols), with = FALSE],
                   by = "player_id", all.x = TRUE)

# Join match metadata to get team1/team2 names
match_meta <- merge(margins, match_results[, .(match_id, team1 = team1, team2 = team2)],
                     by = "match_id")

# Aggregate per team per match
team_agg <- pgd_rated[, lapply(.SD, function(x) sum(x, na.rm = TRUE)),
                       .SDcols = rating_cols,
                       by = .(match_id, team)]

# Pivot to wide: team1_* and team2_* columns
# Margin is from team1's perspective (innings1 - innings2)
model_data_list <- list()
for (i in seq_len(nrow(match_meta))) {
  mid <- match_meta$match_id[i]
  t1 <- match_meta$team1[i]
  t2 <- match_meta$team2[i]
  margin_val <- match_meta$margin[i]
  inn1 <- match_meta$innings1_total[i]
  inn2 <- match_meta$innings2_total[i]

  t1_ratings <- team_agg[match_id == mid & team == t1]
  t2_ratings <- team_agg[match_id == mid & team == t2]

  if (nrow(t1_ratings) == 0 || nrow(t2_ratings) == 0) next

  row <- data.table(match_id = mid, margin = margin_val,
                     innings1_total = inn1, innings2_total = inn2)

  # Team1 ratings (batting first)
  for (rc in rating_cols) {
    set(row, j = paste0("t1_", rc), value = t1_ratings[[rc]][1])
    set(row, j = paste0("t2_", rc), value = t2_ratings[[rc]][1])
    # Difference features (most predictive for symmetric models)
    set(row, j = paste0("diff_", rc), value = t1_ratings[[rc]][1] - t2_ratings[[rc]][1])
  }
  model_data_list[[length(model_data_list) + 1]] <- row
}

model_data <- rbindlist(model_data_list)
model_data <- model_data[complete.cases(model_data)]

# Use difference features for the margin model (symmetric: team1 - team2)
diff_cols <- grep("^diff_", names(model_data), value = TRUE)
t1_cols <- grep("^t1_", names(model_data), value = TRUE)
t2_cols <- grep("^t2_", names(model_data), value = TRUE)

cat(sprintf("Model data: %d matches, %d diff features, %d total features\n",
    nrow(model_data), length(diff_cols), length(diff_cols) + length(t1_cols) + length(t2_cols)))

# ============================================================================
# Train glmnet models
# ============================================================================

cli::cli_h2("Training glmnet models")

# --- Model 1: Margin ~ diff_ratings (symmetric) ---
X_diff <- as.matrix(model_data[, diff_cols, with = FALSE])
X_diff_sd <- apply(X_diff, 2, sd)
X_diff_sd[X_diff_sd == 0] <- 1
X_diff_scaled <- scale(X_diff, center = TRUE, scale = X_diff_sd)

y_margin <- model_data$margin
cv_margin <- glmnet::cv.glmnet(X_diff_scaled, y_margin, alpha = 0.5, nfolds = 10)
coef_margin <- as.numeric(coef(cv_margin, s = "lambda.min"))[-1]

margin_df <- data.frame(
  stat_name = sub("^diff_", "", sub("_rating$", "", diff_cols)),
  beta = round(coef_margin, 6),
  sd = round(X_diff_sd, 6),
  stringsAsFactors = FALSE
)

cat(sprintf("Margin model: %d non-zero coefficients\n", sum(coef_margin != 0)))
cv_mse <- cv_margin$cvm[cv_margin$lambda == cv_margin$lambda.min]
cat(sprintf("CV R²: %.3f (MSE: %.1f)\n", 1 - cv_mse / var(y_margin), cv_mse))

# --- Model 2: Batting (innings 1 total ~ t1 ratings) ---
X_t1 <- as.matrix(model_data[, t1_cols, with = FALSE])
X_t1_sd <- apply(X_t1, 2, sd)
X_t1_sd[X_t1_sd == 0] <- 1
X_t1_scaled <- scale(X_t1, center = TRUE, scale = X_t1_sd)

y_bat <- model_data$innings1_total
cv_bat <- glmnet::cv.glmnet(X_t1_scaled, y_bat, alpha = 0.5, nfolds = 10)
coef_bat <- as.numeric(coef(cv_bat, s = "lambda.min"))[-1]

batv_df <- data.frame(
  stat_name = sub("^t1_", "", sub("_rating$", "", t1_cols)),
  beta = round(coef_bat, 6),
  sd = round(X_t1_sd, 6),
  stringsAsFactors = FALSE
)
cat(sprintf("Batting model: %d non-zero coefficients, CV R²: %.3f\n",
    sum(coef_bat != 0),
    1 - cv_bat$cvm[cv_bat$lambda == cv_bat$lambda.min] / var(y_bat)))

# --- Model 3: Bowling (innings 2 total ~ t2 ratings, negated) ---
X_t2 <- as.matrix(model_data[, t2_cols, with = FALSE])
X_t2_sd <- apply(X_t2, 2, sd)
X_t2_sd[X_t2_sd == 0] <- 1
X_t2_scaled <- scale(X_t2, center = TRUE, scale = X_t2_sd)

y_bowl <- -model_data$innings2_total
cv_bowl <- glmnet::cv.glmnet(X_t2_scaled, y_bowl, alpha = 0.5, nfolds = 10)
coef_bowl <- as.numeric(coef(cv_bowl, s = "lambda.min"))[-1]

bowlv_df <- data.frame(
  stat_name = sub("^t2_", "", sub("_rating$", "", t2_cols)),
  beta = round(coef_bowl, 6),
  sd = round(X_t2_sd, 6),
  stringsAsFactors = FALSE
)
cat(sprintf("Bowling model: %d non-zero coefficients, CV R²: %.3f\n",
    sum(coef_bowl != 0),
    1 - cv_bowl$cvm[cv_bowl$lambda == cv_bowl$lambda.min] / var(y_bowl)))

# ============================================================================
# Save coefficients
# ============================================================================

cli::cli_h2("Saving coefficients")

extdata_dir <- file.path(getwd(), "inst", "extdata")
dir.create(extdata_dir, recursive = TRUE, showWarnings = FALSE)

write.csv(margin_df, file.path(extdata_dir, "psr_coefficients.csv"), row.names = FALSE)
write.csv(batv_df, file.path(extdata_dir, "batv_coefficients.csv"), row.names = FALSE)
write.csv(bowlv_df, file.path(extdata_dir, "bowlv_coefficients.csv"), row.names = FALSE)

cli::cli_alert_success("Saved psr_coefficients.csv ({sum(margin_df$beta != 0)} non-zero)")
cli::cli_alert_success("Saved batv_coefficients.csv ({sum(batv_df$beta != 0)} non-zero)")
cli::cli_alert_success("Saved bowlv_coefficients.csv ({sum(bowlv_df$beta != 0)} non-zero)")

# Show top coefficients
cat("\n=== Top PSR margin coefficients (by |beta|) ===\n")
top_coef <- margin_df[order(-abs(margin_df$beta)), ]
print(head(top_coef[top_coef$beta != 0, ], 15))

cli::cli_h1("PSR Model Training Complete")
