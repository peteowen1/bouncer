# Scratch location holding the per-variant scores from the fit script.
OUT <- commandArgs(trailingOnly = TRUE)[1]
if (is.na(OUT) || !nzchar(OUT)) OUT <- file.path(Sys.getenv("TEMP", unset = tempdir()), "agnostic_venue_resources")
b <- readRDS(file.path(OUT, "exp59_baseline.rds"))
v <- readRDS(file.path(OUT, "exp59_venue.rds"))
r <- readRDS(file.path(OUT, "exp59_resources.rds"))
bo <- readRDS(file.path(OUT, "exp59_both.rds"))
d <- b$row_ll - v$row_ll          # positive = venue is better on that ball
n <- length(d)
se <- sd(d) / sqrt(n)
cat(sprintf("held-out balls          %s\n", format(n, big.mark = ",")))
cat(sprintf("baseline  mlogloss      %.6f\n", b$mlogloss))
cat(sprintf("+ venue   mlogloss      %.6f  (%+.4f%%)\n", v$mlogloss, -100*mean(d)/b$mlogloss))
cat(sprintf("+ resources             %.6f  (%+.4f%%)\n", r$mlogloss, 100*(r$mlogloss-b$mlogloss)/b$mlogloss))
cat(sprintf("+ both                  %.6f  (%+.4f%%)\n", bo$mlogloss, 100*(bo$mlogloss-b$mlogloss)/b$mlogloss))
cat(sprintf("\npaired mean gain        %.3e  (SE %.3e, t = %.1f)\n", mean(d), se, mean(d)/se))
cat(sprintf("gain as %% of the ~0.4%% headroom   %.1f%%\n",
            100 * (100*mean(d)/b$mlogloss) / 0.4))
