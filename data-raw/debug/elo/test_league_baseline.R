# Test league baseline calculations
devtools::load_all()

cat('Package loaded successfully\n\n')

# Test league baseline calculation
cat('Testing league baseline calculations:\n')
cat('LEAGUE_BASELINE_BLEND_HALFLIFE =', LEAGUE_BASELINE_BLEND_HALFLIFE, '\n\n')

cat('New league (500 deliveries, 1.35 avg):',
    calculate_league_baseline(1.35, 500, 1.28), '\n')
cat('Established league (5000 deliveries, 1.35 avg):',
    calculate_league_baseline(1.35, 5000, 1.28), '\n')
cat('Mature league (20000 deliveries, 1.35 avg):',
    calculate_league_baseline(1.35, 20000, 1.28), '\n')
cat('Low-scoring league (10000 deliveries, 1.05 avg):',
    calculate_league_baseline(1.05, 10000, 1.28), '\n')
cat('No data:', calculate_league_baseline(NA, 0, 1.28), '\n')
