suppressMessages(library(data.table))
OUT <- "C:/Users/peteo/AppData/Local/Temp/claude/C--dev-bouncerverse/635fc43f-1352-411b-8c7d-693d0ebc00b2/scratchpad/test_lambda"
o <- as.data.table(readRDS(file.path(OUT, "ridge_sweep.rds")))

cat("=== evaluation rows per bucket/role (summed over 3 origins, one ridge value) ===\n")
print(dcast(o[ridge == 60], bucket ~ role, value.var = "n"))

cat("\n=== per-origin spread at the shipped ridge 60 -- is the gain stable in time? ===\n")
w <- dcast(o[ridge == 60], bucket + role ~ origin, value.var = "gain")
print(w[, lapply(.SD, function(x) if (is.numeric(x)) round(x, 1) else x)])

cat("\n=== range across origins (max - min) at ridge 60 ===\n")
r <- o[ridge == 60, .(spread = round(max(gain) - min(gain), 1),
                      mean = round(mean(gain), 1)), by = .(bucket, role)]
print(r[order(-spread)])
cat("\n  A spread far larger than the mean means the 'gain' is not a stable\n")
cat("  property -- it depends which year you look at.\n")
