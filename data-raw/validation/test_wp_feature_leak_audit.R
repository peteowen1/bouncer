# Leakage audit of every Test WP v3 feature.
#
# The test: at the FIRST BALL of a match nothing about the match has happened,
# so no feature may correlate with the eventual outcome beyond what venue
# history and the schedule legitimately explain. A feature that both VARIES at
# ball one and predicts the result is carrying the future.
setwd("C:/dev/bouncerverse/bouncer")
suppressMessages(devtools::load_all(quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
v3 <- readRDS("C:/dev/bouncerverse/bouncerdata/models/test_winprob_v3_results.rds")
feats <- union(v3$result_features, v3$conditional_features)
cat(sprintf("features across both models: %d\n\n", length(feats)))

# Rebuild the training frame by sourcing the trainer up to the feature block.
src <- readLines("data-raw/models/in-match/08_test_win_probability_v3.R")
stop_at <- grep("Train/Test Split", src, fixed = TRUE)[1]
if (is.na(stop_at)) stop_at <- grep("result_features <- c", src, fixed = TRUE)[1]
e <- new.env()
suppressMessages(suppressWarnings(eval(parse(text = paste(src[1:(stop_at-1)], collapse = "\n")), envir = e)))
d <- e$deliveries
cat(sprintf("frame rebuilt: %s rows\n", format(nrow(d), big.mark=",")))
saveRDS(d[, .SD, .SDcols = intersect(c(feats, "match_id","innings","match_outcome","is_result","cum_overs"), names(d))],
        file.path(tempdir(), "audit_frame.rds"))

first <- d[innings == 1 & cum_overs < 1]
cat(sprintf("first-over-of-match rows: %s across %s matches\n\n",
            format(nrow(first), big.mark=","), format(uniqueN(first$match_id), big.mark=",")))

cat(sprintf("%-26s %8s %10s %s\n", "feature", "varies", "cor(result)", "verdict"))
cat(strrep("-", 66), "\n")
res <- list()
for (f in sort(feats)) {
  if (!f %in% names(first)) { cat(sprintf("%-26s %8s %10s %s\n", f, "-", "-", "ABSENT")); next }
  x <- as.numeric(first[[f]]); y <- as.numeric(first$is_result)
  nu <- uniqueN(x)
  cc <- if (nu <= 1 || sd(x, na.rm=TRUE) == 0) NA_real_ else suppressWarnings(cor(x, y, use="complete.obs"))
  verdict <- if (nu <= 1) "constant - cannot leak"
             else if (is.na(cc)) "no variance"
             else if (abs(cc) >= 0.30) "*** INVESTIGATE"
             else if (abs(cc) >= 0.15) "  check"
             else "ok"
  cat(sprintf("%-26s %8d %10s %s\n", f, nu,
              if (is.na(cc)) "-" else sprintf("%+.3f", cc), verdict))
  res[[f]] <- cc
}
