# Player Network Centrality
#
# Main entry point for network centrality and quality analysis.
#
# This module has been split into focused files for maintainability:
#
#   - centrality_core.R: Matrix building, network calculation algorithms
#     - build_matchup_matrices()
#     - find_bipartite_components()
#     - normalize_pagerank_by_component()
#     - normalize_pagerank_by_opponent_diversity()
#     - calculate_unique_opponent_counts()
#     - calculate_network_centrality()
#     - classify_centrality_tiers()
#
#   - centrality_pagerank.R: PageRank algorithm (legacy, prefer centrality)
#     - compute_cricket_pagerank()
#     - classify_pagerank_tiers()
#
#   - centrality_interface.R: High-level database functions
#     - calculate_player_pagerank()
#     - get_top_pagerank_players()
#     - compare_pagerank_elo()
#     - print_pagerank_summary()
#     - calculate_player_centrality()
#     - print_centrality_summary()
#
#   - centrality_history.R: Snapshot storage and retrieval
#     - ensure_centrality_history_table()
#     - store_centrality_snapshot()
#     - get_centrality_as_of()
#     - batch_get_centrality_for_match()
#     - get_centrality_snapshot_dates()
#     - delete_old_centrality_snapshots()
#     - Legacy PageRank history functions (for backwards compatibility)
#
#   - centrality_integration.R: ELO system integration
#     - get_cold_start_percentile()
#     - get_centrality_k_multiplier()
#     - calculate_league_starting_elo()
#     - calculate_centrality_regression()
#     - build_event_centrality_lookup()
#     - get_player_debut_event()
#     - batch_get_player_debut_events()
#
# Constants are defined in constants_centrality.R:
#   - CENTRALITY_ALPHA, CENTRALITY_MIN_DELIVERIES
#   - CENTRALITY_K_FLOOR/CEILING/MIDPOINT/STEEPNESS
#   - CENTRALITY_COLD_START_TIER_1-4
#   - CENTRALITY_REGRESSION_STRENGTH
#   - CENTRALITY_ELO_PER_PERCENTILE
#   - CENTRALITY_SNAPSHOT_KEEP_MONTHS
#
# All functions are available via the bouncer package namespace.

NULL
