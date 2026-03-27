# Bouncer Data Dictionary

Column definitions for all DuckDB tables and computed features in the bouncer cricket analytics pipeline. Tables are per-format (`t20`, `odi`, `test`) unless noted.

---

## 1. Core Match Data (`cricsheet.matches`)

| Column | Type | Description |
|--------|------|-------------|
| `match_id` | integer | Unique match identifier (Cricsheet registry number) |
| `match_type` | character | `T20`, `IT20`, `ODI`, `ODM`, `Test`, `MDM` |
| `match_date` | date | Match start date |
| `season` | character | Season/year identifier |
| `gender` | character | `male` or `female` |
| `venue` | character | Ground/stadium name |
| `city` | character | City name |
| `team1` | character | First team listed (usually batting first) |
| `team2` | character | Second team |
| `toss_winner` | character | Team winning the toss |
| `toss_decision` | character | `bat` or `field` |
| `outcome_winner` | character | Winning team (NULL for ties/draws/no result) |
| `outcome_type` | character | `wins`, `tie`, `no result`, `draw` |
| `outcome_method` | character | `DLS`, `bowl-out`, etc. (NULL if normal) |
| `event_name` | character | Tournament/series name |
| `event_match_number` | integer | Match number within event |
| `is_knockout` | logical | Knockout/playoff match (computed from event metadata) |
| `event_tier` | integer | 1-4 quality tier (1=ICC events, 4=associate) |
| `missing_data` | logical | Data completeness flag |

## 2. Ball-by-Ball Data (`cricsheet.deliveries`)

| Column | Type | Description |
|--------|------|-------------|
| `delivery_id` | character | Unique ID: `"{match_id}_{batting_team}_{innings}_{over:03d}_{ball:02d}"` |
| `match_id` | integer | Foreign key to `cricsheet.matches` |
| `innings` | integer | 1 = batting first, 2 = chasing (3/4 for Tests) |
| `batting_team` | character | Team batting this delivery |
| `bowling_team` | character | Team bowling this delivery |
| `over` | integer | Over number (0-indexed in raw data) |
| `ball` | integer | Ball within over (0-5) |
| `batter_id` | character | Cricsheet player registry ID — batter on strike |
| `bowler_id` | character | Cricsheet player registry ID — bowler |
| `non_striker_id` | character | Batter at non-striker's end |
| `runs_batter` | integer | Runs scored by batter (0-6, excludes extras) |
| `runs_extras` | integer | Extra runs (wides, no-balls, byes, leg-byes) |
| `runs_total` | integer | Total runs from this delivery (`runs_batter + runs_extras`) |
| `is_wicket` | logical | TRUE if a batsman was dismissed |
| `wicket_kind` | character | Dismissal type: bowled, caught, lbw, run out, stumped, etc. |
| `player_out_id` | character | Player dismissed (may differ from batter for run outs) |
| `is_wide` | logical | Wide ball |
| `is_noball` | logical | No-ball |
| `is_extra` | logical | Any extra delivery |

### Supplementary Delivery Columns

| Column | Type | Description |
|--------|------|-------------|
| `has_review` | logical | DRS review taken on this delivery |
| `review_by` | character | Team requesting review |
| `review_decision` | character | Umpire decision after review |
| `has_replacement` | logical | Player replacement occurred |
| `replacement_in` | character | Incoming replacement player |
| `replacement_out` | character | Outgoing player |
| `replacement_reason` | character | `injury`, `absent`, etc. |

## 3. Player Skill Indices (`main.{format}_player_skill`)

Residual-based EMA tracking how much a player deviates from the agnostic baseline expectation. Updated per delivery.

| Column | Type | Start | Description |
|--------|------|-------|-------------|
| `delivery_id` | character | — | Foreign key to deliveries |
| `batter_id` | character | — | Player batting |
| `bowler_id` | character | — | Player bowling |
| `batter_scoring_index` | numeric | 0 | Runs deviation from expected (residual EMA). Positive = scores more than expected |
| `batter_survival_rate` | numeric | ~0.95 | Probability of surviving each ball (absolute EMA on 0/1 outcome) |
| `batter_balls_faced` | integer | 0 | Cumulative deliveries faced (drives adaptive alpha) |
| `bowler_economy_index` | numeric | 0 | Runs conceded deviation (residual EMA). Negative = more economical than expected |
| `bowler_strike_rate` | numeric | ~0.05 | Wicket-taking probability per ball (absolute EMA) |
| `bowler_balls_bowled` | integer | 0 | Cumulative deliveries bowled |

**Update formula:** `new = (1 - alpha) * old + alpha * observation`
- Indices use `residual` as observation (deviation from agnostic expected)
- Rates use raw outcome (0 or 1 for survival/wicket)
- Alpha decreases with experience: `alpha = alpha_min + (alpha_max - alpha_min) * exp(-deliveries / halflife)`

## 4. Team Skill Indices (`main.{format}_team_skill`)

Same EMA approach as player skills but at team level. Alpha = 50% of player alpha (teams change composition slowly).

| Column | Type | Description |
|--------|------|-------------|
| `batting_team_runs_skill` | numeric | Runs deviation when batting (positive = scores more) |
| `batting_team_wicket_skill` | numeric | Wicket rate deviation when batting (negative = loses fewer) |
| `bowling_team_runs_skill` | numeric | Runs deviation when bowling (negative = concedes fewer) |
| `bowling_team_wicket_skill` | numeric | Wicket rate deviation when bowling (positive = takes more) |
| `batting_team_balls` | integer | Cumulative balls batted by team |
| `bowling_team_balls` | integer | Cumulative balls bowled by team |

## 5. Venue Skill Indices (`main.{format}_venue_skill`)

Per-venue characteristics. Lowest alpha (venues change slowest). Split into session (short-term) and permanent (long-term).

| Column | Type | Method | Description |
|--------|------|--------|-------------|
| `venue_run_rate` | numeric | Residual EMA | Runs deviation from format average |
| `venue_wicket_rate` | numeric | Residual EMA | Wicket deviation from format average |
| `venue_boundary_rate` | numeric | Raw EMA | Boundary (4s + 6s) probability — no agnostic baseline |
| `venue_dot_rate` | numeric | Raw EMA | Dot ball probability — no agnostic baseline |
| `venue_balls` | integer | — | Total deliveries at venue |
| `canonical_venue` | character | — | Standardized venue name (handles aliases) |

## 6. 3-Way ELO (`main.{format}_3way_elo`)

Per-delivery ELO ratings decomposing each outcome into batter, bowler, and venue contributions. All start at 1400, bounded 1000-1800 (venue permanent: 1200-1600).

### Run ELO Columns

| Column | Type | Description |
|--------|------|-------------|
| `batter_run_elo_before` / `_after` | numeric | Batter's run-scoring ELO (higher = scores more) |
| `bowler_run_elo_before` / `_after` | numeric | Bowler's run-prevention ELO (higher = concedes fewer) |
| `venue_perm_run_elo_before` / `_after` | numeric | Long-term venue run tendency (~3yr decay) |
| `venue_session_run_elo_before` / `_after` | numeric | Short-term pitch/conditions effect (resets each match to 1400) |

### Wicket ELO Columns

| Column | Type | Description |
|--------|------|-------------|
| `batter_wicket_elo_before` / `_after` | numeric | Batter's survival ELO (higher = harder to dismiss) |
| `bowler_wicket_elo_before` / `_after` | numeric | Bowler's wicket-taking ELO (higher = takes more) |
| `venue_perm_wicket_elo_before` / `_after` | numeric | Long-term venue wicket tendency |
| `venue_session_wicket_elo_before` / `_after` | numeric | Short-term pitch wicket effect |

### K-Factor Columns

| Column | Type | Description |
|--------|------|-------------|
| `k_batter_run` | numeric | Dynamic learning rate for batter (decreases with experience) |
| `k_bowler_run` | numeric | Dynamic learning rate for bowler |
| `k_venue_perm_run` | numeric | Dynamic learning rate for venue permanent |
| `k_venue_session_run` | numeric | Dynamic learning rate for venue session |
| `days_inactive_batter` | numeric | Days since last match (drives decay toward replacement level) |
| `days_inactive_bowler` | numeric | Days since last match |

### Attribution Weights (format-gender specific)

| Component | Men's T20 | Men's ODI | Men's Test | Description |
|-----------|-----------|-----------|------------|-------------|
| `W_BATTER` | 0.612 | varies | varies | Batter's share of outcome |
| `W_BOWLER` | 0.311 | varies | varies | Bowler's share |
| `W_VENUE_SESSION` | 0.062 | varies | varies | Short-term pitch/conditions |
| `W_VENUE_PERM` | 0.015 | varies | varies | Long-term ground characteristics |

Weights differ by format, gender, and dimension (run vs wicket). See `get_run_elo_weights()` / `get_wicket_elo_weights()` in `R/constants_3way.R`.

## 7. Score Projection (`main.{format}_score_projection`)

Projects final innings totals from any game state. Used for margin calculation and live predictions.

| Column | Type | Description |
|--------|------|-------------|
| `delivery_id` | character | Foreign key |
| `current_score` | integer | Runs scored at this delivery |
| `wickets_fallen` | integer | Wickets lost |
| `balls_bowled` | integer | Balls faced in innings |
| `balls_remaining` | integer | Balls left (0 for Tests) |
| `wickets_remaining` | integer | `10 - wickets_fallen` |
| `resource_remaining` | numeric | Resources left (0-1), see formula below |
| `resource_used` | numeric | `1 - resource_remaining` |
| `eis_agnostic` | integer | Expected Initial Score — format average |
| `eis_full` | integer | Expected Initial Score — team/venue adjusted |
| `projected_agnostic` | integer | Projected total using format average |
| `projected_full` | integer | Projected total using team/venue skills |
| `projection_change_agnostic` | integer | Change from previous delivery |
| `projection_change_full` | integer | Change from previous delivery |
| `final_innings_total` | integer | Actual final score (for validation) |

## 8. Team ELO (`main.team_elo`)

Game-level team ratings based on match results.

| Column | Type | Description |
|--------|------|-------------|
| `match_id` | integer | Match identifier |
| `team` | character | Team name |
| `elo_result` | numeric | Result-based ELO rating |
| `elo_roster_combined` | numeric | Aggregated ELO from individual player ratings |
| `matches_played` | integer | Experience indicator |

## 9. Feature Engineering (Computed Columns)

Columns computed by `R/feature_engineering.R` and added to delivery data during pipeline processing.

### Phase & Context

| Column | Type | Description |
|--------|------|-------------|
| `phase` | factor | `powerplay`, `middle`, `death` (T20/ODI) or `new_ball`, `middle`, `old_ball` (Test) |
| `overs_into_phase` | numeric | Overs elapsed in current phase |
| `overs_completed` | numeric | Decimal overs completed (e.g., 10.3) |
| `overs_remaining` | numeric | Overs left in innings (NA for Tests) |
| `balls_remaining` | integer | Balls left |
| `total_runs` | integer | Cumulative innings score |
| `wickets_fallen` | integer | Cumulative wickets in innings |
| `runs_difference` | integer | `team1_score - current_score` (2nd innings pressure) |
| `is_wicket_int` | integer | Integer version (0/1) of `is_wicket` |
| `is_four` | logical | 4 runs scored |
| `is_six` | logical | 6 runs scored |
| `is_boundary` | integer | `as.integer(is_four | is_six)` |
| `is_dot` | integer | `as.integer(runs_total == 0 & !is_wicket)` |

### Rolling Momentum

| Column | Type | Description |
|--------|------|-------------|
| `runs_last_12_balls` | integer | Runs in last 2 overs |
| `runs_last_24_balls` | integer | Runs in last 4 overs |
| `dots_last_12_balls` | integer | Dot balls in last 2 overs |
| `dots_last_24_balls` | integer | Dot balls in last 4 overs |
| `boundaries_last_12_balls` | integer | Boundaries in last 2 overs |
| `boundaries_last_24_balls` | integer | Boundaries in last 4 overs |
| `wickets_last_12_balls` | integer | Wickets in last 2 overs |
| `wickets_last_24_balls` | integer | Wickets in last 4 overs |
| `runs_last_3_overs` | integer | Runs in last 3 overs |
| `runs_last_6_overs` | integer | Runs in last 6 overs |
| `rr_last_3_overs` | numeric | Run rate over last 3 overs |
| `rr_last_6_overs` | numeric | Run rate over last 6 overs |
| `over_runs` | integer | Runs in current over so far |
| `over_wickets` | integer | Wickets in current over so far |

### Chase Metrics (2nd innings only)

| Column | Type | Description |
|--------|------|-------------|
| `runs_needed` | integer | Runs still required to win |
| `wickets_in_hand` | integer | `10 - wickets_fallen` |
| `required_run_rate` | numeric | Runs per over needed to win |
| `rr_differential` | numeric | Required RR minus current RR |
| `balls_per_run_needed` | numeric | Balls available per run required |
| `balls_per_wicket_available` | numeric | Balls per remaining wicket |
| `chase_buffer` | integer | `balls_remaining - runs_needed` |
| `chase_buffer_ratio` | numeric | `chase_buffer / balls_remaining` |
| `theoretical_min_balls` | integer | `ceiling(runs_needed / 6)` — minimum balls to reach target |
| `balls_surplus` | integer | `balls_remaining - theoretical_min_balls` |
| `is_easy_chase` | integer | `< 1 run/ball needed AND 5+ wickets in hand` |
| `is_difficult_chase` | integer | `> 2 runs/ball needed OR < 3 wickets left` |
| `is_death_chase` | logical | Last 4 overs of a chase |
| `chase_completed` | integer | 1 if `runs_needed <= 0` |
| `chase_impossible` | integer | 1 if no resources but runs still needed |

## 10. Win Probability & Attribution

Columns from `R/win_probability_added.R` and `R/player_attribution.R`.

| Column | Type | Description |
|--------|------|-------------|
| `predicted_before` | numeric | Model win probability before this delivery |
| `predicted_after` | numeric | Model win probability after this delivery |
| `wpa` | numeric | Win Probability Added: `predicted_after - predicted_before` |
| `batter_wpa` | numeric | WPA credited to batter |
| `bowler_wpa` | numeric | WPA credited to bowler |
| `expected_runs` | numeric | Expected runs per ball from model |
| `era` | numeric | Expected Runs Added: `actual - expected` |
| `batter_era` | numeric | ERA credited to batter |
| `bowler_era` | numeric | ERA credited to bowler |

## 11. Pre-Match Prediction Features

Aggregated features for match-level prediction model. Built by `R/pre_match_features.R`.

| Column | Type | Description |
|--------|------|-------------|
| `team1_elo_result` | numeric | Team 1 match-result ELO |
| `team2_elo_result` | numeric | Team 2 match-result ELO |
| `elo_diff_result` | numeric | `team1_elo - team2_elo` |
| `team1_elo_roster` | numeric | Team 1 roster-aggregated ELO |
| `team2_elo_roster` | numeric | Team 2 roster-aggregated ELO |
| `team1_form_last5` | numeric | Team 1 win rate in last 5 matches |
| `team2_form_last5` | numeric | Team 2 win rate in last 5 matches |
| `team1_h2h_total` | integer | Head-to-head matches played |
| `team1_h2h_wins` | integer | Team 1 head-to-head wins |
| `team1_bat_scoring_avg` | numeric | Average batting scoring index across roster |
| `team1_bat_scoring_top5` | numeric | Top 5 batters' average scoring index |
| `team1_bat_survival_avg` | numeric | Average batter survival rate |
| `team1_bowl_economy_avg` | numeric | Average bowler economy index |
| `team1_bowl_economy_top5` | numeric | Top 5 bowlers' economy index |
| `team1_bowl_strike_avg` | numeric | Average bowler strike rate |
| `bat_scoring_diff` | numeric | `team1 - team2` batting index difference |
| `bowl_economy_diff` | numeric | `team2 - team1` bowling economy difference |
| `team1_won_toss` | logical | Team 1 won the toss |
| `toss_elect_bat` | logical | Toss winner elected to bat |
| `venue_avg_score` | integer | Historical average score at venue |
| `venue_chase_success_rate` | numeric | Historical chase success rate at venue |
| `team1_team_runs_skill` | numeric | Team 1 batting skill index |
| `team2_team_runs_skill` | numeric | Team 2 batting skill index |

(Columns repeated symmetrically for `team2_*` where applicable.)

## 12. Cricinfo Hawkeye Data (`cricinfo.balls`)

Optional tracking data from ESPN Cricinfo. Available for ~3,700 matches with ball-by-ball Hawkeye data.

| Column | Type | Description |
|--------|------|-------------|
| `wagonX` | numeric | Wagon wheel x coordinate (where ball went) |
| `wagonY` | numeric | Wagon wheel y coordinate |
| `wagonZone` | character | Field zone the ball was hit to |
| `pitchLine` | character | `off`, `leg`, `middle` — where ball pitched |
| `pitchLength` | character | `full`, `good`, `short`, `yorker` — length |
| `shotType` | character | `drive`, `cut`, `pull`, `defense`, etc. |
| `shotControl` | character | `controlled`, `risky`, `very risky` |
| `win_probability` | numeric | Hawkeye-computed win probability |

## 13. Centrality / Network Quality

From `R/centrality.R` and `R/centrality_storage.R`. PageRank-based quality adjustment for ELO ratings.

| Column | Type | Description |
|--------|------|-------------|
| `batter_centrality` | numeric | PageRank score (0-1). Low = inflated ELO from weak opponents |
| `bowler_centrality` | numeric | PageRank score for bowler |
| `unique_opponents` | integer | Distinct opponents faced |
| `avg_opponent_degree` | numeric | Average opponent network connectivity |

## 14. Margin & Outcome

From `R/margin_calculation.R`. Converts all match outcomes to a runs-equivalent margin.

| Column | Type | Description |
|--------|------|-------------|
| `margin` | numeric | Runs-equivalent margin (positive = team1 won) |
| `win_type` | character | `runs`, `wickets`, `tie`, `draw`, `no_result` |
| `wickets_remaining` | integer | For wickets wins: wickets in hand when chase completed |
| `overs_remaining` | numeric | Cricket notation (e.g., 2.3 = 2 overs 3 balls remaining) |

---

## Key Formulas

### Skill Index Update (per delivery)
```
residual = actual_outcome - agnostic_expected
new_skill = (1 - alpha) * old_skill + alpha * residual
```

### Adaptive Alpha (learning rate)
```
alpha = alpha_min + (alpha_max - alpha_min) * exp(-deliveries / halflife)
```
New players learn fast (high alpha), experienced players are stable (low alpha).

### 3-Way ELO Expected Runs
```
expected_runs = agnostic_baseline * (1 + (w_batter * (batter_elo - 1400)
                                        + w_bowler * (1400 - bowler_elo)
                                        + w_venue_perm * (venue_perm - 1400)
                                        + w_venue_session * (venue_session - 1400))
                                       * runs_per_100_elo)
```

### Score Projection
```
resource_remaining = (balls_remaining / max_balls)^z * (wickets_remaining / 10)^y
projected = current_score + a * eis * resource_remaining
          + b * current_score * resource_remaining / resource_used
```
Parameters `a`, `b`, `z`, `y` are optimized per format-gender-team_type segment.

### Unified Margin (wickets wins → runs equivalent)
Wickets wins are converted to runs-equivalent using the score projection system, allowing all results to be compared on the same scale.

---

## Format-Specific Constants

| Constant | T20 | ODI | Test |
|----------|-----|-----|------|
| Expected runs/ball | 1.138 | 0.782 | 0.518 |
| Expected wicket/ball | 5.4% | 2.8% | 1.7% |
| Max balls per innings | 120 | 300 | Variable |
| Skill alpha (player) | 0.01 | 0.008 | 0.005 |
| Skill alpha (venue) | 0.002 | 0.001 | 0.0005 |
| ELO start | 1400 | 1400 | 1400 |
| Runs per 100 ELO | 0.0745 | 0.0826 | 0.0932 |
