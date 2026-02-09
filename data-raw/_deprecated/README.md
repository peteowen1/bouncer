# Deprecated Code Archive

This directory contains code that is no longer actively maintained but is preserved for reference.

## Contents

### `dual-elo/`
**Archived:** February 2026

The original dual ELO rating system that tracked separate ELO ratings for:
- Batters: survival ELO (staying in) vs scoring ELO (runs per ball)
- Bowlers: strike ELO (taking wickets) vs economy ELO (runs conceded)

**Why deprecated:** Replaced by the 3-Way ELO system which provides better attribution
by including venue effects and using a more sophisticated K-factor structure.

**Files:**
- `01_calculate_dual_elos.R` - Main calculation script
- `02_apply_elo_normalization.R` - Rating normalization
- `03_validate_dual_elos.R` - Validation checks
- `run_pipeline.R` - Pipeline runner

### `three_way_glicko.R`
**Archived:** February 2026

A Glicko-style rating system that extended 3-Way ELO with explicit uncertainty tracking
via Rating Deviation (RD).

**Why deprecated:** The explicit uncertainty tracking added complexity without providing
sufficient predictive benefit over the simpler 3-Way ELO system. The ELO system's
implicit uncertainty via dynamic K-factors proved sufficient for our use case.

**Key concepts preserved:**
- g-function for weighting updates by opponent uncertainty
- RD decay with inactivity
- Fisher Information-based RD updates

## Migration Notes

If you need any of this functionality:
1. The 3-Way ELO system (`R/three_way_elo.R`) is the current recommended approach
2. For uncertainty estimation, consider bootstrap methods or ensemble predictions
3. For dual metrics (scoring vs survival), use the skill index system (`R/*_skill_index.R`)

## Do Not Use

Code in this directory:
- Is not tested or maintained
- May reference functions that no longer exist
- Should not be restored to the active codebase without careful review
