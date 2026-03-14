// Fast ELO Loss Calculation for Optimization
//
// C++ implementation of the inner ELO simulation loop.
// Called via Rcpp::sourceCpp() from the optimization scripts.
//
// This replaces the pure R for-loop which processes 200k deliveries
// sequentially with hash table lookups. The C++ version uses
// std::unordered_map for O(1) lookups and runs ~50-100x faster.
//
// Usage from R:
//   Rcpp::sourceCpp("data-raw/ratings/player/3way-elo/optimization/elo_loss_fast.cpp")
//   loss <- calculate_run_poisson_loss_cpp(params, batter_ids, bowler_ids,
//             venues, match_ids, baseline_runs, actual_runs, elo_start)

#include <Rcpp.h>
#include <unordered_map>
#include <string>
#include <cmath>

using namespace Rcpp;

// [[Rcpp::export]]
double calculate_run_poisson_loss_cpp(
    NumericVector params,
    CharacterVector batter_ids,
    CharacterVector bowler_ids,
    CharacterVector venues,
    CharacterVector match_ids,
    NumericVector baseline_runs,
    NumericVector actual_runs,
    double elo_start) {

  int n = actual_runs.size();

  // Extract parameters
  double k_run_max = params[0];
  double k_run_min = params[1];
  double k_run_halflife = params[2];
  double k_venue_perm_max = params[3];
  double k_venue_perm_min = params[4];
  double k_venue_perm_halflife = params[5];
  double k_venue_session_max = params[6];
  double k_venue_session_min = params[7];
  double k_venue_session_halflife = params[8];
  double w_batter = params[9];
  double w_bowler = params[10];
  double w_venue_session = params[11];
  double w_venue_perm = params[12];
  double runs_per_100_elo = params[13];

  // Normalize weights
  double w_total = w_batter + w_bowler + w_venue_session + w_venue_perm;
  w_batter /= w_total;
  w_bowler /= w_total;
  w_venue_session /= w_total;
  w_venue_perm /= w_total;

  double runs_per_elo = runs_per_100_elo / 100.0;

  // ELO stores
  std::unordered_map<std::string, double> batter_elos;
  std::unordered_map<std::string, double> bowler_elos;
  std::unordered_map<std::string, double> venue_perm_elos;

  // Experience counters
  std::unordered_map<std::string, int> batter_balls;
  std::unordered_map<std::string, int> bowler_balls;
  std::unordered_map<std::string, int> venue_balls;

  // Venue session state
  std::string current_match = "";
  double venue_session_elo = elo_start;
  int match_balls = 0;

  // Accumulate loss
  double loss_sum = 0.0;

  for (int i = 0; i < n; i++) {
    std::string bat_id = as<std::string>(batter_ids[i]);
    std::string bowl_id = as<std::string>(bowler_ids[i]);
    std::string venue = as<std::string>(venues[i]);
    std::string match_id_str = as<std::string>(match_ids[i]);

    // Reset session on new match
    if (match_id_str != current_match) {
      current_match = match_id_str;
      venue_session_elo = elo_start;
      match_balls = 0;
    }

    // Get current ELOs (default to start)
    double batter_elo = batter_elos.count(bat_id) ? batter_elos[bat_id] : elo_start;
    double bowler_elo = bowler_elos.count(bowl_id) ? bowler_elos[bowl_id] : elo_start;
    double venue_perm_elo = venue_perm_elos.count(venue) ? venue_perm_elos[venue] : elo_start;

    // Get experience
    int batter_exp = batter_balls.count(bat_id) ? batter_balls[bat_id] : 0;
    int bowler_exp = bowler_balls.count(bowl_id) ? bowler_balls[bowl_id] : 0;
    int venue_exp = venue_balls.count(venue) ? venue_balls[venue] : 0;

    // Calculate expected runs
    double baseline = baseline_runs[i];
    double batter_contrib = (batter_elo - elo_start) * runs_per_elo;
    double bowler_contrib = (elo_start - bowler_elo) * runs_per_elo;
    double venue_perm_contrib = (venue_perm_elo - elo_start) * runs_per_elo;
    double venue_session_contrib = (venue_session_elo - elo_start) * runs_per_elo;

    double expected = baseline +
      w_batter * batter_contrib +
      w_bowler * bowler_contrib +
      w_venue_perm * venue_perm_contrib +
      w_venue_session * venue_session_contrib;

    // Bound
    if (expected < 0.01) expected = 0.01;
    if (expected > 6.0) expected = 6.0;

    // Accumulate Poisson loss: pred - actual * log(pred)
    double actual = actual_runs[i];
    loss_sum += expected - actual * std::log(expected);

    // Calculate residual
    double delta = actual - expected;

    // K-factors
    double k_batter = k_run_min + (k_run_max - k_run_min) * std::exp(-(double)batter_exp / k_run_halflife);
    double k_bowler_val = k_run_min + (k_run_max - k_run_min) * std::exp(-(double)bowler_exp / k_run_halflife);
    double k_venue_perm = k_venue_perm_min + (k_venue_perm_max - k_venue_perm_min) * std::exp(-(double)venue_exp / k_venue_perm_halflife);
    double k_venue_sess = k_venue_session_max * std::exp(-(double)match_balls / k_venue_session_halflife);
    if (k_venue_sess < k_venue_session_min) k_venue_sess = k_venue_session_min;

    // Update ELOs
    batter_elos[bat_id] = batter_elo + k_batter * delta;
    bowler_elos[bowl_id] = bowler_elo - k_bowler_val * delta;
    venue_perm_elos[venue] = venue_perm_elo + k_venue_perm * delta;
    venue_session_elo += k_venue_sess * delta;

    // Update experience
    batter_balls[bat_id] = batter_exp + 1;
    bowler_balls[bowl_id] = bowler_exp + 1;
    venue_balls[venue] = venue_exp + 1;
    match_balls++;
  }

  return loss_sum / (double)n;
}
