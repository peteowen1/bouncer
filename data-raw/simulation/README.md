# Cricket Match & Season Simulation

## Overview

This module provides a framework for simulating cricket matches, seasons, and playoffs. It uses pre-match prediction probabilities to run Monte Carlo simulations and project outcomes.

## Status

**Framework Only** - Core infrastructure is in place, but simulation scripts are stubs awaiting implementation.

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

- Requires: `data-raw/predictive-modelling/` (pre-match predictions)
- Requires: bouncer package functions via `devtools::load_all()`

## Future Enhancements

- [ ] Ball-by-ball simulation (detailed)
- [ ] What-if scenarios (roster changes)
- [ ] Live match simulation updates
- [ ] Tournament bracket visualization
- [ ] Historical validation against actual outcomes
