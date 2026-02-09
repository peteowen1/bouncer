# Release Scripts

Scripts for managing GitHub releases and data distribution.

## Scripts

### upload_cricsheet_release.R
Uploads processed cricsheet data to GitHub releases for distribution.

**Usage:**
```r
source("data-raw/release/upload_cricsheet_release.R")
```

### full_refresh_core.R
Performs a complete refresh of core data tables, typically used before major releases.

**Usage:**
```r
source("data-raw/release/full_refresh_core.R")
```

## Release Workflow

1. Ensure all pipeline steps have completed successfully
2. Run `full_refresh_core.R` if needed
3. Run `upload_cricsheet_release.R` to create/update GitHub release
4. Verify release at https://github.com/peteowen1/bouncerdata/releases

## Notes

- Releases are uploaded to the `bouncerdata` repo, not `bouncer`
- Uses `BOUNCERDATA_PAT` for cross-repo authentication
- Large files may take several minutes to upload
