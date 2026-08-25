# bouncer 0.7.5

## Ball-outcome models trained on wides, no-balls and free hits (#81/D-P50)

Wides are now a trained category in both the agnostic and full outcome
models (`OUTCOME_CATEGORIES`), instead of being silently excluded from
training or -- for the full model -- silently mislabeled as dot balls.
`is_free_hit` is derived and backfilled onto every cricsheet delivery and
wired in as a training feature. Both models retrained cleanly across
T20/ODI/Test; a reproducible native OpenMP crash in Test's `xgb.cv()` was
root-caused and fixed along the way (`nthread=4`).

The match simulator (`simulate_delivery()`/`simulate_innings()`) now models
illegal deliveries: a wide is drawn from the model's own trained category, a
no-ball is drawn independently at a measured per-format rate (the model was
never given a feature for it, since a no-ball's runs distribution mirrors a
legal ball's), and only a no-ball grants a free hit -- matching real cricket
law and this package's own `compute_is_free_hit()` derivation. Wicket
occurrence on a no-ball is also drawn independently, since only a run-out is
legal there and the model's unconditional wicket probability overstated
that by 9-45x.

RAA scoring (`build_cricsheet_raa()`) now passes `is_free_hit` through to
the agnostic model; the existing wides-exclusion filter is unchanged (a
wide is never a "ball faced").

## IPL baseline projected score no longer leaks the venue average (bouncerverse#82)

`02_baseline_projected_score.R`'s "par score" averaged every match at a
venue including the one being predicted. Fixed with the same time-causal
construction already used for the T20/ODI and Test in-match leaks.

## Causal per-day rain features added to the Test win-probability model (bouncerverse#72)

`rain_mm_before`/`rain_days_before`/`venue_rain_climatology` replace a
disabled stub, backfilled from 285 venues of daily weather history. Ball-
level results are mixed (see the project decision log) and shipped because
the code itself is correct and honestly evaluated, not because it's a clear
win.

## `cricsheet.players.country` no longer described as a nationality (bouncerverse#77)

No player-nationality field exists anywhere in cricsheet's data; the column
is kept (still meaningful as "first team seen") but relabeled honestly.

## Prior-innings declaration flags added to the Test win-probability model (bouncerverse#78)

Training-only for now -- cricinfo, the live serving source, has no
`declared` field.

# bouncer 0.7.4

## T20/ODI in-match models no longer leak venue-average features (bouncerverse#80)

`venue_avg_score`/`venue_chase_success_rate` used to average over EVERY match
at a venue, including the one being predicted -- the same leak already found
and fixed for Test format. Switched to the time-causal construction built for
that fix; sized directly (correlation with the match's own score at single-
match venues was 1.000, dropping sharply once fixed). All six affected
in-match models (t20/odi x stage1/innings1/stage2) retrained and republished.

# bouncer 0.7.3

## Full outcome model is reachable and safe to use externally (bouncerverse#76, #79)

`load_full_model()`, `load_agnostic_model()`, `predict_full_outcome()` and
`predict_agnostic_outcome()` are now exported. Doing that safely required
closing a real gap first: xgboost's `predict()` is positional, so a serving
frame with the same columns in a different order than training silently
returns plausible, wrong numbers -- no error, no warning. The three published
full models had this fixed by re-stamping their training-order feature list
onto the already-trained boosters (verified via a byte-identical prediction
round-trip, no retrain needed), and `predict_full_outcome()`/
`predict_agnostic_outcome()` now check a serving frame against that stamp on
every call, aborting loudly on a mismatch instead of relying on a standalone
script nobody was forced to run.

# bouncer 0.7.2

## Added a reusable calibration-and-bias audit for models and ratings

`calibration_audit()`, `worst_calibration_buckets()`, and
`audit_low_information_state()` (`R/calibration_audit.R`) generalise the check
that caught the post-delivery leak in the ball-outcome model (below): bucket
any prediction/actual pair by any cut (ball, over, innings, wickets, phase,
competition, venue, gender, format, season, ...), refuse to judge a bucket
below a caller-set `min_n` rather than silently dropping it, surface the
worst buckets by `abs(bias)` rather than the average, and explicitly check a
caller-nominated low-information state (e.g. the first ball of an innings)
for the correlation-with-own-outcome signature a leak leaves behind. See
`docs/reference/MODEL-VALIDATION-PROTOCOL.md` for the full protocol and the
three incidents it is built from.

## Restored the four versebus.R silent-failure fixes, one of them corrected

These four fixes shipped in 86e2ebc, then were entirely backed out same-day in
5edd3ac for two reasons: `test-versebus-sync.R` requires this vendored file to
match torp's canonical copy byte-for-byte function-by-function, and fixing it
here alone broke that guard; and one of the four fixes was itself wrong.

`vb_download()`'s `verify_by_size()` originally swallowed an asset-listing
failure and did nothing -- the fix in 86e2ebc made that **abort**
(`vb_error_transient`) instead, but that inverts the caller's own contract:
the sha-mismatch path deliberately falls back to `verify_by_size()` on a stale
(not corrupt) manifest, and `test-versebus.R` already documents that "cannot
corroborate either way" means *trust the download*. Aborting there would
brick an asset on any transient API blip. The real defect was only that the
fallback was silent. It now warns (naming the file, stating it was accepted
WITHOUT verification) and still falls through -- behaviour preserved, silence
removed.

The other three fixes are unchanged from 86e2ebc:

* `vb_read_manifest()`'s momentary-absence retry classified every retry
  failure as "the manifest is gone," permanently disabling sha256
  verification for the rest of the session on a single network blip. The
  retry is now classified exactly like the first attempt.
* `vb_generation()` took `max()` of asset `updated_at` with no `na.rm`, so one
  unrelated malformed asset nulled the generation for the whole tag.
* `vb_publish()`'s cache-invalidation hook failure was swallowed by
  `try(..., silent = TRUE)`, so a broken hook left consumers serving
  pre-publish data with nothing recording why. It now warns.

Because torp's canonical copy had also drifted independently since 86e2ebc --
`.vb_generation_stamp()` no longer calls `sample()` (which silently advanced
the caller's RNG stream) and `vb_publish()` now restores
`piggyback_cache_duration` on exit instead of leaking it for the session --
those two changes are pulled in too, or `test-versebus-sync.R` cannot pass.
Neither is a behaviour change for any caller outside this file.

`R/user_install.R`'s `download_release_asset()` fix (never unlink the
destination before the swap) was bouncer-specific and was never backed out;
untouched here.

Refs peteowen1/panna#187.

# bouncer 0.7.1

## The competition adjustment had the wrong sign for below-average players

`fit_competition_factors()` estimates a ratio of batting **averages** -- a
non-negative quantity where a ratio is natural. The rating applied it as
`(raa - opp_eff) / cfactor`, and RVAA is a **signed deviation**, so dividing a
negative by 1.6 moved it toward zero: the weak-league discount made a
below-average batter look *better*. In T20 men, **671 of 1,039** below-average
batters with 200+ balls were being helped, by up to +0.201 RVAA/ball.

The rating now recentres onto the reference scale before compressing:

```r
value <- m_ref + (v0 - m_here) / cfactor
```

`m_here` is what an average bridge player scores in that competition and
`m_ref` what the same players score in the reference, both from the new
`fit_competition_offsets()`. Offsets are fitted per role on `raa - opp_eff`,
because that is what they are subtracted from -- weak competitions are full of
weak bowlers and `fit_two_way_effects()` already removes part of a
competition's strength, so fitting on raw RVAA would double-discount.

On next-match Spearman over reference matches: batters **+2.6%** overall and
**+19.3%** for players whose records are 60%+ weak-league cricket; bowlers
**+0.9%** and **+5.8%**. All ten anchor checks pass; old-vs-new rank
correlation 0.916.

Plain additive recentring was built first and rejected on an anchor. A flat
offset is not progressive, so it correctly stopped rewarding below-average
weak-league batters while *easing* the discount on the best one -- moving a
batter with 1,354 balls and no reference cricket from 7th to 4th in the world.
With compression he sits 28th.

The compression term's stated cause is **withdrawn**: the 1.35x spread ratio
behind it was between-competition variance in an estimate that never centred
each competition. Measured within competition, weak-league spreads are smaller
than reference spreads. The term is kept on an anchor and a metric, and the
resulting crossover -- below which a weak-competition return still rates above
the same return in the reference -- is pinned by `test-competition-adjust.R`
rather than hidden.

### Also

* `metric = "wickets"` fits its competition factor on the survival basis again.
  Rewiring the application site dropped the argument, so a wickets rating was
  compressing WAA deviations with a batting-average factor. Not live (the
  pipeline runs `metric = "composite"`), but reachable.
* Both the factor-basis and offset-side guards run before the connection opens,
  so a mismatched argument fails in milliseconds rather than after 2M rows.

# bouncer 0.7.0

## A post-delivery target leak was in every ball-outcome model

`cricsheet.deliveries.total_runs` is the innings score **including** the current
ball. Eleven queries took it raw as `batting_score` and derived
`runs_difference` from it, feeding models whose target is that same ball's
outcome. On the first ball of an innings nothing else is in the feature, so it
*was* the target.

* `cor(runs_difference, runs off that ball)` = **1.000** across 14,129 T20
  innings. 5,812 of 9,700 first balls predicted `E[runs] < 0.05`, and their
  actual mean was **0.000** against **1.844** for the rest.
* The effect dilutes as the score grows — `cor(pred, actual)` runs 0.442 at over
  0 against ~0.175 by over 8 — which is why over-level calibration looked healthy
  the whole time.
* All three formats retrained. **mlogloss got worse everywhere** (T20 1.3805 →
  1.4137, ODI 1.1634 → 1.1871, Test 0.9252 → 0.9288), which is what removing a
  leak must do. First-ball `sd(E[runs])` fell 17x in T20.
* A second leak of the same shape was found in review: `R/model_predictions.R`
  had the runs fix applied and left `wickets_fallen` raw in the same `SELECT`,
  feeding a model that predicts `P(wicket)`. Fixed there and in three trainers.
  **Those models must be retrained before their `pred_*` columns are used.**
* Training also included wides while the RAA scorer excluded them. Populations
  now match row-for-row.

**Every rating computed before this is superseded.**

## Competition factors rebuilt

Cricsheet names every bilateral series as its own event, so the factor was
fitted **326 times off a median of five matches** — "Zimbabwe in New Zealand
T20I Series" (Williamson, McCullum, Guptill, Taylor) rated the weakest
competition in the fit at 2.90.

* Bilateral tours group into four buckets by playing standard; the 49-event ICC
  qualifying pathway into one. T20 male units **426 → 67**.
* Bridges are weighted by the **harmonic mean** of their two ball counts, which
  is inverse-variance weighting. The old pooled estimator weighted each player
  by his volume on each side separately, so it compared career professionals
  against local players and measured squad composition rather than difficulty.
* Factors shrink toward 1.0 by evidence, and a competition with under one
  weighted dismissal behind it is dropped rather than given a floored
  denominator.
* No domestic league now rates harder than the IPL. Coverage 99.7% / 99.9%.

## The shrinkage prior was derived by the wrong estimator

`derive_shrinkage_prior()` used a one-way ANOVA that **understates the prior in
nine of ten buckets by 28-71%**, so every rating under-shrunk and low-volume
players were over-credited. Now derived by split-half reliability, which needs
no distributional assumption and is deterministic without a seed.

## Also

* Test format added to the rating build. Test **female** is deliberately
  excluded at 46,652 balls over 24 matches.
* The benchmark regression check ran *after* recording, so it compared each run
  against itself and could never fire.
* `derive_shrinkage_prior()` no longer returns a prior whose reported variance
  share disagrees with the prior in force.

# bouncer 0.6.0

## Three ratings instead of one

Runs and wickets are now measured, adjusted and rated separately, and a third
metric prices tempo. `lambda` moves from inside the per-ball score to the
composite, where it can be made situational. See
`bouncerverse/docs/reference/RATING-ARCHITECTURE.md`.

* `main.cricsheet_ball_raa` gains **`waa`** (wickets above average, in wickets,
  carrying no `lambda`) and **`tsa`** (the player's effect on his team's
  projected final score). `waa` was backfilled across 11,045,263 balls without
  rescoring — it is derivable from columns already stored, verified exactly.
* `calculate_player_rating_v2()` takes `metric = "composite" | "runs" |
  "wickets" | "team_score"`. **`composite` is the default and is unchanged**, so
  no published rating moves.
* The three carry genuinely different information: Spearman between the runs and
  wickets ratings is +0.142 in T20 and −0.134 in Test.
* `tsa` is innings 1 of limited-overs only. A chase truncates the innings, so
  projected final score stops being the modelled quantity, and Test has no fixed
  ball allocation.

## Test-format ratings

* **`get_raa_lambda("test")` returns 33**, fitted from actual match outcomes over
  Test + MDM male (5,388,418 deliveries) rather than assumed. A Test can be
  drawn, which the T20/ODI method never had to handle; pricing a draw as a loss
  returns 22.7, *below* ODI, which cannot be right for a format whose innings
  ends on wickets and time rather than balls.
* `.rating_match_types()` and the competition key now support Test, which pairs
  with MDM as ODI pairs with ODM.

## Competition normalisation

* **`COMPETITION_ALIASES` / `alias_competition()`** merge sponsor variants of one
  competition. England's domestic T20 was split three ways across 1,554
  matches — more than the IPL — and five other competitions were similarly
  split. Unlike `COMPETITION_UNIT_MAP` this is a rename, not a partition: an
  unlisted competition passes through unchanged.
* **`COMPETITION_REFERENCE_ODI_FEMALE` was anchored on a competition that ended
  in 2024.** The Rachael Heyhoe Flint Trophy's successor carries the ECB name
  and was not in the reference set, so from 2025 that cricket was unanchored.
  A test now fails if any reference set names a retired alias.
* `fit_competition_factors()` takes `basis = "runs" | "survival"`. A batting
  average is the wrong yardstick for a survival metric, and weak leagues inflate
  scoring far more than they inflate survival.

## Correctness

* **`as_at` now truncates before fitting**, not only at the final aggregation.
  It previously left the opponent effects and competition factors fitted on the
  whole corpus, which is harmless for a current rating and a leak in any
  backtest. Verified not to move the shipped path: `as_at = NULL` reproduces the
  prior rating to 1.07e-14.
* `derive_shrinkage_prior()` aborts when the between-player variance is not
  identified, instead of returning a prior of order 1e11 that collapses every
  player onto the population mean while preserving rank order.

# bouncer 0.5.0

## Bowling figures: run outs are no longer the bowler's wicket

Four functions counted every dismissal on a bowler's delivery as his wicket,
including run outs, which are nobody's. This inflated T20 wickets by **9.7%**
and understated bowling averages by **1.94 runs** — and because it is not a
uniform rescaling, it *reordered* bowlers rather than just shifting them. A
minor release rather than a patch: every published bowling average moves.

* `query_bowler_stats()`, `analyze_match()` and the player-metrics queries now
  count only bowler-credited kinds — caught, bowled, lbw, caught and bowled,
  stumped, hit wicket. `analyze_match()` was the last holdout and it orders its
  list by wickets, so a bowler could outrank a colleague purely because a run
  out happened on his over, and disagree with `query_bowler_stats()` for the
  same match.

## Ratings carry the traditional numbers, and derive their own prior

* `average` and `main_comp` on every rating, so a leaderboard can be read
  against the number people already know. `player_career_context()` supplies
  them; a batter with no dismissals or a bowler with no wickets reports `NA`,
  never a fabricated zero or an `Inf`.
* `derive_shrinkage_prior()` estimates the shrinkage prior per bucket by
  unbalanced one-way ANOVA instead of reusing a men's-T20 20. T20 male batting
  derives **39.9** where the next-match harness independently prefers **40**.
* **`derive_shrinkage_prior()` now refuses a bucket whose between-player
  variance is not identified.** It previously floored the variance at `1e-9`
  and returned whatever fell out, which reproduces the "145 billion matches"
  prior its own documentation described as already fixed. That value is not a
  visibly broken number: every player collapses onto the population mean, so
  the leaderboard still ranks in the right order with fabricated spread, and a
  rank-based anchor check cannot see it. It now aborts, and warns when the
  implied player share of single-match variance falls outside 0.5–25%.
* The thin-bucket fallback says so explicitly instead of logging
  `NA% of single-match variance is the player`.

## Correctness

* **`find_player()`** — look a player up by name without silently getting the
  wrong one. Wanindu Hasaranga is `PWH de Silva`; Varun Chakaravarthy is
  `CV Varun`. Returns every candidate, best-evidenced first, and warns when
  more than one matches rather than quietly taking the first.
* **Three seeded splits are now reproducible.** DuckDB does not guarantee row
  order, so `set.seed()` on a query result reproduces nothing without a stable
  sort first. The score-projection sampler sorts on `delivery_id`, the only
  unique key available: an earlier form sorted on `over * 6 + ball`, which is
  not unique because `ball` counts extras and runs past 6, so over 0 ball 7 and
  over 1 ball 1 collide.
* **Rating queries refuse an unsupported format instead of guessing.** The
  match-type selector was a `t20` branch and an ODI catch-all, so
  `format = "test"` would have returned ODI deliveries labelled Test. Test is
  the next bucket queued, which is when that trap was most likely to fire.
* `player_career_context()` returns a typed empty table when its query matches
  nothing, so the caller's merge fails naming the real cause rather than a
  missing key.

# bouncer 0.4.0

## Player Rating v2

A rebuilt player rating, running off Cricsheet rather than Cricinfo, adjusted
for the opponent faced and the competition played in, and covering men's and
women's T20 and ODI. Decisions D-P16 to D-P29.

* `calculate_player_rating_v2()` rates batters and bowlers on per-ball runs
  above average, net of the opponent (a two-way alternating-ridge fit) and
  divided by a competition difficulty factor. Selected throughout by
  out-of-sample next-match Spearman, never by leaderboard appearance.
* `calculate_player_value_v2()` combines batting and bowling into one
  per-match-played value, built as quality x opportunity so a specialist's
  batting term reads near zero rather than inheriting an average batter's.
* `fit_competition_factors()` derives competition strength from bridge players
  — the only construction that identifies it, since players and competitions
  are not crossed — anchored on a per-bucket reference set and chained outward.
* `fit_two_way_effects()` exposes the crossed batter/bowler fit.
* `build_player_id_map()` / `canonicalise_player_ids()` merge careers split
  across a bare-name id and a registry hash id.
* `store_player_rating_v2()`, `store_player_value_v2()` and
  `load_player_rating_v2()` persist and read the results.

## Win probability and match state

* `build_cricsheet_win_probability()`, `build_cricinfo_win_probability()` and
  `build_cricinfo_test_win_probability()` score win probability from bouncer's
  own models.
* `fit_resource_surface()` replaces the assumption that a wicket is worth
  exactly six balls with a fitted expected-remaining-runs surface.
* `build_cricsheet_raa()` writes per-ball runs above average.
* `calculate_impact()` supersedes `calculate_epr()`, which remains as a
  deprecated alias.

## Bug fixes

* Player careers split across two ids are merged before rating; 3,775 registry
  ids were the player's name rather than a hash, affecting 2,903 players.
* `download_release_asset()` no longer deletes the destination before the
  swap, which on a failed swap destroyed both the old and the new file.
* Rating writes run in a transaction, so a failed insert cannot leave a bucket
  empty.
* Competition-factor chaining no longer counts a bridge player once per
  neighbouring competition, and clamps at each step rather than only at the end.
* Several silent failures in the versebus download path now raise or warn.
* WPA deltas come from each ball's own pre-delivery state, and are credited to
  the batter's own team.

# bouncer 0.3.0

## Major Changes

* **3-Way ELO system** - New primary rating system with separate ELO ratings for
  batter, bowler, and venue (session + permanent) on every delivery. Attribution
  weights: batter 52%, bowler 22%, venue session 21%, venue permanent 5%.

* **PageRank/centrality quality adjustment** - Network-based system that detects
  inflated ratings from isolated competitions. New functions:
  `get_centrality_as_of()`, `get_pagerank_as_of()`, `get_top_pagerank_players()`,
  `calculate_player_centrality()`, `apply_centrality_correction()`.

* **Score projection system** - Per-delivery projected innings totals using
  optimized resource-based models. `calculate_projected_score()` now supports
  all formats with team/venue-adjusted projections.

* **Season & playoff simulation** - `simulate_season()`, `simulate_season_n()`
  for Monte Carlo tournament simulation, plus `simulate_ipl_playoffs()` for
  IPL-specific playoff brackets.

* **Glicko system deprecated** - Removed in favour of the 3-Way ELO + centrality
  approach. Legacy code archived in `data-raw/_deprecated/`.

## Improvements

* `install_all_bouncer_data()` provides one-step data installation from GitHub
  releases, replacing the multi-step manual process.

* New constant getter functions (`get_skill_alpha()`, `get_run_elo_weights()`,
 `get_venue_k_factors()`, etc.) replace hard-coded values with format-aware
  configuration.

* Remote data loading now supported - load data directly from GitHub releases
  without a local DuckDB database.

## Internal

* Major file reorganization: split monolithic files into domain modules
  (database, ELO, centrality, constants, user API).
* Removed ~10 dead code files (old dual-ELO player processing pipeline).
* Tightened exports: ~14 internal functions un-exported, replaced with
  proper getter APIs.
* SQL injection fixes in database query functions.
* Comprehensive test coverage for ELO, simulation, and user API.

# bouncer 0.1.0

Initial CRAN-ready release of bouncer - Cricket analytics with player skill indices.

## Data Management

* `install_bouncer_data()`, `install_all_bouncer_data()` - Download Cricsheet data and store in DuckDB database
* `install_bouncerdata_from_release()` - Install pre-processed data from GitHub releases
* `update_bouncerdata()` - Incremental data updates
* `connect_to_bouncer()`, `disconnect_bouncer()` - Database connection management
* `load_matches()`, `load_deliveries()`, `load_players()`, `load_innings()` - Load core tables into R

## Skill Index System

Novel residual-based skill tracking system that updates ball-by-ball:

* **Player skills**: `load_player_skill()` - Batting scoring index, survival rate, bowling economy, strike rate
* **Team skills**: `load_team_skill()` - Aggregate batting/bowling ability relative to baseline
* **Venue skills**: `load_venue_skill()` - Ground characteristics (run rate, wicket rate, boundaries)

Skill indices represent deviations from format-specific baselines (T20/ODI/Test), allowing cross-format comparisons.

## Player Analysis

* `get_player()` - Player lookup with current skill indices
* `analyze_player()` - Comprehensive player breakdown (batting, bowling, skill history)
* `compare_players()` - Head-to-head player comparison
* `search_players()` - Find players by partial name
* `rank_players()` - Player rankings by skill index
* `player_batting_stats()`, `player_bowling_stats()` - Aggregated career statistics
* `analyze_batter_vs_bowler()` - Specific matchup analysis

## Team Analysis

* `get_team()` - Team lookup with ELO ratings
* `compare_teams()` - Head-to-head team comparison
* `search_teams()` - Find teams by partial name
* `head_to_head()` - Historical team matchup record
* `team_batting_stats()`, `team_bowling_stats()` - Team performance aggregates

## Venue Analysis

* `venue_stats()` - Venue performance characteristics

## Match Analysis

* `analyze_match()` - Comprehensive match breakdown
* `query_matches()` - Search/filter matches

## ELO Ratings

* `get_team_elo()` - Team ELO ratings (game-level)
* `load_team_elo()` - Historical team ELO data

## Predictions

* `predict_match()` - Pre-match win probability
* `predict_match_outcome()` - Match result prediction with confidence
* `predict_win_probability()` - In-game win probability
* `calculate_projected_score()` - Innings score projection
* `calculate_projection_resource()` - Duckworth-Lewis style resource percentage

## Simulation

* `simulate_match_ballbyball()` - Full match simulation
* `quick_match_simulation()` - Fast match simulation
* `simulate_innings()` - Single innings simulation
* `simulate_delivery()` - Ball outcome simulation
* `create_simulation_config()` - Configure simulation parameters

## Visualization

* `theme_bouncer()` - Custom ggplot2 theme for cricket charts
* `plot_score_progression()` - Innings scoring worm
* `plot_win_probability()` - Win probability over time
* `plot_skill_progression()` - Player skill evolution
* `plot_player_comparison()` - Visual player comparison
* `plot_elo_history()` - ELO rating history
* `plot_team_strength()` - Team strength visualization

## Database Queries

Advanced users can query the database directly:

* `query_deliveries()` - Ball-by-ball data queries
* `query_batter_stats()`, `query_bowler_stats()` - Aggregated statistics
* `query_player_stats()` - Combined player statistics

## Test Cricket Utilities

* `calculate_test_projected_score()` - Test match score projection
* `calculate_test_overs_remaining()` - Remaining overs estimation
* `estimate_test_innings_overs_remaining()` - Innings overs estimation

## Data Parsing

* `parse_cricsheet_json()` - Parse Cricsheet JSON files
