# Which of those gains survive their own between-origin variation?
# Standard error of the mean across origins; a cell whose |mean| is smaller than
# ~2 SE is not distinguishable from "the adjustments do nothing".
suppressMessages(library(data.table))
OUT <- "C:/Users/peteo/AppData/Local/Temp/claude/C--dev-bouncerverse/635fc43f-1352-411b-8c7d-693d0ebc00b2/scratchpad/test_lambda"
o <- as.data.table(readRDS(file.path(OUT, "full_sweep.rds")))

s <- o[, .(k = .N, mean = mean(gain), se = sd(gain) / sqrt(.N)), by = .(bucket, role, ridge)]
s[, t := mean / se]
s[, verdict := fifelse(abs(t) >= 2, "real", "not distinguishable from 0")]

cat("=== at the SHIPPED ridge 60 ===\n")
print(s[ridge == 60][order(role, -mean),
        .(bucket, role, origins = k, mean = round(mean,1), se = round(se,1),
          t = round(t,1), verdict)])

cat("\n=== at each cell's BEST ridge ===\n")
b <- s[, .SD[which.max(mean)], by = .(bucket, role)]
print(b[order(role, -mean),
        .(bucket, role, ridge, mean = round(mean,1), se = round(se,1),
          t = round(t,1), verdict)])

cat("\n=== how much does the RIDGE choice matter, within a cell? ===\n")
r <- s[, .(best = round(max(mean),1), worst = round(min(mean),1),
           swing = round(max(mean) - min(mean),1),
           best_ridge = ridge[which.max(mean)]), by = .(bucket, role)]
print(r[order(role, -swing)])
cat("\n  'swing' is what choosing the ridge is worth. Compare it to the SE above:\n")
cat("  if swing < 2*SE the parameter cannot be chosen from this evidence.\n")
