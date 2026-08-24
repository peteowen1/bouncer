# Do batting and bowling ratings compose, or does one swallow the other?
# Evidence for the team-rating design (bouncerverse#60, question 1).
#
# THE PRECEDENT. D-P7 found that EPR's WPA term was 0.009% of its variance --
# `bat_value = batting_wpa + batting_era` was simply ERA, so every WPA
# improvement was inert and tuning it could not move a rating. The same shape
# is worth checking before any team rating SUMS a batting and a bowling number.
#
# MEASURED 2026-08-21, male, genuine all-rounders only (200+ balls both ways,
# so specialists cannot drive it):
#
#   format  players  bat_sd  bowl_sd  bat share of variance  cor(total, bat)
#   T20         605   1.551    2.024                 37.0%            +0.386
#   ODI         683   2.257    4.552                 19.7%            +0.157
#   TEST        993   3.613   12.617                  7.6%            -0.085
#
# Restricting to all-rounders makes it WORSE than the full population, so this
# is not a specialist artefact -- it is the composite itself.
#
# In Test, main.player_value_v2.total_value IS bowl_value: batting is 7.6% of
# the summed variance and the composite correlates -0.085 with it. A team
# rating built by summing total_value would be a bowling-only team rating that
# looks like a complete one.
#
# This does NOT prescribe a fix. Standardising the two components was tried for
# EPR and made the anchors worse (D-P7), so the answer is a design decision,
# not an obvious rescale.

# Is total_value ~ bowl_value an artefact of specialists, or real?
suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
conn <- get_db_connection(read_only = TRUE); on.exit(dbDisconnect(conn, shutdown=TRUE), add=TRUE)
v <- as.data.table(dbGetQuery(conn, "
  SELECT format, bat_value, bowl_value, total_value, bat_balls, bowl_balls
  FROM main.player_value_v2 WHERE gender='male'"))
v[, `:=`(bat_balls = as.numeric(bat_balls), bowl_balls = as.numeric(bowl_balls))]

cat("role mix:\n")
print(v[, .(all = .N,
            bat_only  = sum(bat_balls > 0 & bowl_balls == 0),
            bowl_only = sum(bowl_balls > 0 & bat_balls == 0),
            genuine_allrounders = sum(bat_balls >= 200 & bowl_balls >= 200)), by = format])

cat("\nGENUINE ALL-ROUNDERS ONLY (200+ balls both ways) -- specialists cannot drive this:\n")
ar <- v[bat_balls >= 200 & bowl_balls >= 200]
print(ar[, .(players = .N,
             bat_sd = round(sd(bat_value),3), bowl_sd = round(sd(bowl_value),3),
             ratio = round(sd(bat_value)/sd(bowl_value), 2),
             cor_total_bat = round(cor(total_value, bat_value), 4),
             cor_total_bowl = round(cor(total_value, bowl_value), 4),
             bat_var_share = round(100*var(bat_value)/(var(bat_value)+var(bowl_value)), 1)),
         by = format])
cat("\nbat_var_share = batting's share of the summed variance. 50% would be balanced.\n")
