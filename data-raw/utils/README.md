# Utility Scripts

Maintenance and utility scripts that don't fit into the main pipeline categories.

## Scripts

### clean_fox_cache.R
Cleans cached Fox Sports data to free disk space or force fresh downloads.

**Usage:**
```r
source("data-raw/utils/clean_fox_cache.R")
```

**When to use:**
- Cache has grown too large
- Need to re-scrape data due to format changes
- Debugging scraping issues

## Adding New Utilities

Utility scripts should:
1. Be self-contained (load their own dependencies)
2. Have clear documentation at the top
3. Not modify database tables without explicit confirmation
4. Follow the standard script template:

```r
# Utility: <name>
# Purpose: <brief description>
# Usage: source("data-raw/utils/<name>.R")

library(DBI)
devtools::load_all()

# Utility code here
```
