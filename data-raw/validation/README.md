# Validation Scripts

Production data quality checks designed to run regularly (e.g., after pipeline updates).
Unlike debug scripts, these are maintained, tested, and expected to pass.

## Purpose

These scripts verify:
- Pipeline step outputs exist and are valid
- Data quality meets expectations (no unexpected NULLs, valid ranges)
- Database schema matches expectations
- Cross-table consistency

## Usage

Run individual checks:
```r
source("data-raw/validation/validate_pipeline_state.R")
source("data-raw/validation/validate_data_quality.R")
```

Run all checks:
```r
source("data-raw/validation/run_all_checks.R")
```

## Scripts

### `validate_pipeline_state.R`
Verifies that all pipeline steps have completed successfully:
- Core tables exist and have expected row counts
- Skill tables are populated for all formats
- ELO tables have recent data

### `validate_data_quality.R`
Checks data quality across key tables:
- No unexpected NULL values in required columns
- Values within expected ranges (overs, balls, wickets)
- No duplicate primary keys

### `validate_schema.R`
Verifies database schema matches expectations:
- All expected tables exist
- Column types are correct
- Indexes are present

### `run_all_checks.R`
Runs all validation scripts and reports summary.

## When to Run

- After `run_full_pipeline.R` completes
- After incremental data updates
- Before releasing new package versions
- When investigating data issues

## Adding New Checks

1. Create a new `validate_*.R` script
2. Use `cli::cli_alert_*` for output
3. Return TRUE/FALSE for pass/fail
4. Add to `run_all_checks.R`
