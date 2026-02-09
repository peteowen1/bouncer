# Cricket Match & Season Simulation

## Overview

This module provides a framework for simulating cricket matches, seasons, and playoffs. It uses the full ball-outcome model to run ball-by-ball Monte Carlo simulations.

## Status

**Implemented** - Ball-by-ball simulation is fully functional via `R/simulation.R` package functions.

## Quick Start

```r
devtools::load_all()

# 1. Set up simulation framework (required first)
source("data-raw/simulation/01_simulation_framework.R")

# 2. Simulate individual matches
source("data-raw/simulation/02_match_simulation.R")

# 3. Simulate full seasons
source("data-raw/simulation/03_season_simulation.R")

# 4. Simulate playoff brackets
source("data-raw/simulation/04_playoff_simulation.R")

# 5. Analyze simulation results
source("data-raw/simulation/05_simulation_analysis.R")
```

## Simulation Types

### 1. Match Simulation (`02_match_simulation.R`)
- Simulate a single match N times
- Returns win probability distribution
- Methods: ELO-based, roster-based, or full model

### 2. Season Simulation (`03_season_simulation.R`)
- Simulate remaining season matches
- Project final standings
- Calculate playoff qualification probabilities
- Track net run rate projections

### 3. Playoff Simulation (`04_playoff_simulation.R`)
- Simulate playoff brackets
- IPL format: Qualifier 1, Eliminator, Qualifier 2, Final
- Calculate championship probabilities

## Database Storage

Results stored in `simulation_results` table:
- `simulation_id`: Unique identifier
- `simulation_type`: "match", "season", "playoffs"
- `n_simulations`: Number of Monte Carlo iterations
- `parameters`: JSON configuration
- `team_results`: JSON with per-team outcomes
- `match_results`: JSON with per-match outcomes

## Dependencies

- Requires: Full outcome model from `data-raw/models/ball-outcome/02_train_full_model.R`
- Requires: Player/team/venue skill indices from `data-raw/ratings/`
- Requires: bouncer package functions via `devtools::load_all()`

## Package Functions

The `R/simulation.R` file provides these simulation functions:

```r
# Ball-by-ball match simulation
simulate_match_ballbyball(model, format, ...)
quick_match_simulation(model, format)

# Innings simulation
simulate_innings(model, format, ...)

# Season simulation
simulate_season(fixtures, n_sims, ...)
simulate_season_n(fixtures, n_sims, ...)

# Playoff simulation
simulate_ipl_playoffs(standings, n_sims, ...)
```

## Future Enhancements

- [ ] What-if scenarios (roster changes)
- [ ] Live match simulation updates
- [ ] Tournament bracket visualization
- [ ] Historical validation against actual outcomes
