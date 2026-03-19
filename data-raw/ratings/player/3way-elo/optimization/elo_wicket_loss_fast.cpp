// Fast Wicket ELO Log-Loss Calculation for Optimization
//
// C++ implementation of the wicket ELO simulation loop.
// Uses binary log-loss (wicket is 0/1 outcome).

#include <Rcpp.h>
#include <unordered_map>
#include <string>
#include <cmath>

using namespace Rcpp;

// [[Rcpp::export]]
double calculate_wicket_logloss_cpp(
    NumericVector params,
    CharacterVector batter_ids,
    CharacterVector bowler_ids,
    CharacterVector venues,
    CharacterVector match_ids,
    NumericVector baseline_wicket,
    IntegerVector is_wicket,
    double elo_start) {

  int n = is_wicket.size();

  // Extract parameters (14 total)
  double k_wicket_max = params[0];
  double k_wicket_min = params[1];
  double k_wicket_halflife = params[2];
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
  double elo_divisor = params[13];

  // Normalize weights
  double w_total = w_batter + w_bowler + w_venue_session + w_venue_perm;
  w_batter /= w_total;
  w_bowler /= w_total;
  w_venue_session /= w_total;
  w_venue_perm /= w_total;

  // ELO stores
  std::unordered_map<std::string, double> batter_elos;
  std::unordered_map<std::string, double> bowler_elos;
  std::unordered_map<std::string, double> venue_perm_elos;

  std::unordered_map<std::string, int> batter_balls;
  std::unordered_map<std::string, int> bowler_balls_map;
  std::unordered_map<std::string, int> venue_balls;

  std::string current_match = "";
  double venue_session_elo = elo_start;
  int match_balls = 0;

  double loss_sum = 0.0;
  double eps = 1e-15;

  for (int i = 0; i < n; i++) {
    std::string bat_id = as<std::string>(batter_ids[i]);
    std::string bowl_id = as<std::string>(bowler_ids[i]);
    std::string venue = as<std::string>(venues[i]);
    std::string match_id_str = as<std::string>(match_ids[i]);

    if (match_id_str != current_match) {
      current_match = match_id_str;
      venue_session_elo = elo_start;
      match_balls = 0;
    }

    double batter_elo = batter_elos.count(bat_id) ? batter_elos[bat_id] : elo_start;
    double bowler_elo = bowler_elos.count(bowl_id) ? bowler_elos[bowl_id] : elo_start;
    double venue_perm_elo = venue_perm_elos.count(venue) ? venue_perm_elos[venue] : elo_start;

    int batter_exp = batter_balls.count(bat_id) ? batter_balls[bat_id] : 0;
    int bowler_exp = bowler_balls_map.count(bowl_id) ? bowler_balls_map[bowl_id] : 0;
    int venue_exp = venue_balls.count(venue) ? venue_balls[venue] : 0;

    // Calculate expected wicket on logit scale
    double baseline = baseline_wicket[i];
    if (baseline < 0.001) baseline = 0.001;
    if (baseline > 0.999) baseline = 0.999;

    double base_logit = std::log(baseline / (1.0 - baseline));

    double batter_contrib = -w_batter * (batter_elo - elo_start) / elo_divisor;
    double bowler_contrib = w_bowler * (bowler_elo - elo_start) / elo_divisor;
    double venue_perm_contrib = w_venue_perm * (venue_perm_elo - elo_start) / elo_divisor;
    double venue_session_contrib = w_venue_session * (venue_session_elo - elo_start) / elo_divisor;

    double adjusted_logit = base_logit + batter_contrib + bowler_contrib +
                            venue_perm_contrib + venue_session_contrib;

    double expected_w = 1.0 / (1.0 + std::exp(-adjusted_logit));
    if (expected_w < 0.001) expected_w = 0.001;
    if (expected_w > 0.999) expected_w = 0.999;

    // Accumulate log-loss
    double actual = (double)is_wicket[i];
    double pred_clipped = std::max(eps, std::min(1.0 - eps, expected_w));
    loss_sum += -(actual * std::log(pred_clipped) + (1.0 - actual) * std::log(1.0 - pred_clipped));

    // K-factors
    double k_bat = k_wicket_min + (k_wicket_max - k_wicket_min) * std::exp(-(double)batter_exp / k_wicket_halflife);
    double k_bowl = k_wicket_min + (k_wicket_max - k_wicket_min) * std::exp(-(double)bowler_exp / k_wicket_halflife);
    double k_vp = k_venue_perm_min + (k_venue_perm_max - k_venue_perm_min) * std::exp(-(double)venue_exp / k_venue_perm_halflife);
    double k_vs = k_venue_session_max * std::exp(-(double)match_balls / k_venue_session_halflife);
    if (k_vs < k_venue_session_min) k_vs = k_venue_session_min;

    // Update ELOs
    batter_elos[bat_id] = batter_elo + k_bat * (expected_w - actual);
    bowler_elos[bowl_id] = bowler_elo + k_bowl * (actual - expected_w);
    venue_perm_elos[venue] = venue_perm_elo + k_vp * (actual - expected_w);
    venue_session_elo += k_vs * (actual - expected_w);

    batter_balls[bat_id] = batter_exp + 1;
    bowler_balls_map[bowl_id] = bowler_exp + 1;
    venue_balls[venue] = venue_exp + 1;
    match_balls++;
  }

  return loss_sum / (double)n;
}
