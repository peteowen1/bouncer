# Publish the cross-competition adjustment as an inspectable matrix (#55).
#
# The adjustment ships as `value = m_ref + (v0 - m_here) / cfactor`, fitted from
# players who appear in two competitions and weighted by the harmonic mean of
# their ball counts on each side (D-P42). That is correct and completely
# opaque: nothing anywhere says how much the system thinks the Nepal Premier
# League differs from the IPL.
#
# EVERY CELL CARRIES ITS EVIDENCE. A cell resting on three bridge players must
# be visibly weaker than one resting on three hundred, or the matrix invites
# exactly the over-reading it exists to prevent.
#
# Usage:
#   Rscript data-raw/ratings/player/rating-v2/build_competition_matrix.R

suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})

OUT_DIR <- "C:/dev/bouncerverse/docs/reference"
MIN_EVIDENCE_TO_SHOW <- 200   # below this a row is reported but flagged thin
MAX_MATRIX_COMPS <- 14        # a 60x60 table is not inspectable

conn <- get_db_connection(read_only = TRUE)
on.exit(dbDisconnect(conn, shutdown = TRUE), add = TRUE)
id_map <- build_player_id_map(conn)

buckets <- list(
  list(format = "t20",  gender = "male"),
  list(format = "odi",  gender = "male"),
  list(format = "test", gender = "male"),
  list(format = "t20",  gender = "female"),
  list(format = "odi",  gender = "female")
)

fit_bucket <- function(format, gender, role) {
  b <- as.data.table(dbGetQuery(conn, sprintf("
    SELECT r.batter_id, r.bowler_id, r.raa, COALESCE(%s,'unknown') AS comp
    FROM main.cricsheet_ball_raa r
    JOIN cricsheet.matches m ON m.match_id = r.match_id
    WHERE r.format = '%s' AND r.gender = '%s'",
    .competition_sql(format), toupper(format), gender)))
  if (!nrow(b)) return(NULL)
  canonicalise_player_ids(b, id_map)

  eff <- fit_two_way_effects(b, prior_balls = 60, iterations = 20)
  if (role == "batter") {
    b[eff$bowler, on = "bowler_id", opp_eff := i.eff]
    id_col <- "batter_id"
  } else {
    b[eff$batter, on = "batter_id", opp_eff := i.eff]
    id_col <- "bowler_id"
  }
  b[is.na(opp_eff), opp_eff := 0]
  b[, v0 := raa - opp_eff]

  off <- fit_competition_offsets(b, id_col, "v0",
                                 default_competition_reference(format, gender))
  fac <- fit_competition_factors(conn, format, gender, id_map = id_map,
                                 basis = "runs")
  off <- merge(off, fac[, .(comp, cfactor = factor)], by = "comp", all.x = TRUE)
  off[is.na(cfactor) | !is.finite(cfactor) | cfactor <= 0, cfactor := 1]

  vol <- b[, .(balls = .N, players = uniqueN(get(id_col))), by = comp]
  off <- merge(off, vol, by = "comp", all.x = TRUE)
  off[is.na(balls), balls := 0L][is.na(players), players := 0L]
  off[]
}

fmt_num <- function(x, d = 2) formatC(x, format = "f", digits = d)

md <- c(
  "# Cross-competition adjustment matrix",
  "",
  sprintf("**Generated %s** by `bouncer/data-raw/ratings/player/rating-v2/build_competition_matrix.R`.",
          Sys.Date()),
  "Regenerate rather than hand-edit.",
  "",
  "This is the adjustment shipped in D-P42, made inspectable. The rating applies",
  "",
  "```",
  "value = m_ref + (v0 - m_here) / cfactor",
  "```",
  "",
  "estimated **only** from players who appear in two competitions, weighted by the",
  "harmonic mean of their ball counts on each side (inverse-variance weighting),",
  "chained outward from the reference set and shrunk toward neutral by evidence.",
  "",
  "## The two numbers, and which one is better identified",
  "",
  "| column | what it is | how well identified |",
  "|---|---|---|",
  "| **inflation** (`offset`) | How many more runs per 100 balls **the same player** scores here than in reference cricket. Positive means easier. | **Paired, within player.** This is what the bridge construction actually measures, and the part D-P40 fixed by weighting pairs instead of pooling two population averages. |",
  "| **assigned level** (`m_ref`) | What the rating credits an *average* player in this competition with, on the reference scale. | Derived as `m_here - offset`. Inherits `m_here` — the competition's mean among its bridge players — which depends on who happens to play there. |",
  "",
  "**The matrix below is built from `offset`**, because that is what the question",
  "\"how do we adjust between leagues\" actually asks: what the same player scores",
  "in one place versus the other. It is antisymmetric by construction.",
  "",
  "### What `m_ref` turned out to track",
  "",
  "Worth knowing before reading the assigned-level column. Across the 59 T20 male",
  "competitions with real bridge evidence, `m_ref` correlates **+0.728** (Spearman)",
  "with the share of that competition's balls played by people who *also* play",
  "reference cricket. `offset` correlates **-0.715** with the same thing.",
  "",
  "That is reassuring rather than alarming: a league where 93% of the batting is",
  "done by players who also appear in top franchise cricket really is a strong",
  "league, so an assigned level tracking it is behaving. Major League Cricket (93%",
  "bridged) and International (Top Nations) (98%) land within 1.3 runs/100 balls of",
  "each other at the top; Syed Mushtaq Ali (43% bridged) sits below both.",
  "",
  "Where it misbehaves is the rows already flagged. The Stan Nagaiah Trophy has the",
  "highest assigned level in the whole T20 male table on **14 players, 0% bridged,",
  "and a chained (step 1) estimate** — the chaining error compounding, which is",
  "exactly what `step` and the evidence flag exist to warn about. A thin chained",
  "row is not a claim.",
  "",
  "## How to read a cell",
  "",
  "`cell(A -> B)` is how many more runs per 100 balls **the same player** scores in",
  "A than in B. Positive means A is the **easier** place to bat.",
  "",
  "`cell(A -> B) = (offset_A - offset_B) * 100`. Antisymmetric: the cell for",
  "B -> A is its negation, and the diagonal is zero.",
  "",
  "So to move a return from A to B, subtract the cell. A player averaging 30 runs",
  "per 100 balls in a competition 20 runs/100 easier than the one you care about is",
  "doing what a 10-per-100 player does there.",
  "",
  "## What a cell does NOT tell you",
  "",
  "- **This is scoring, not purely standard.** A player scores less on a seaming",
  "  pitch than a flat one, and the bridge construction removes *player* selection",
  "  but not *conditions*. D-P22 is the precedent: runs per ball, dismissals per",
  "  ball and runs per wicket all rank the IPL the **easiest** place to bat,",
  "  because it has the best pitches. Expect conditions to be in these numbers.",
  "- **Thin cells are barely claims at all.** Read the evidence column first. A",
  "  competition bridged by three players is a guess with a number attached; rows",
  "  under 200 balls of harmonic bridge evidence are marked.",
  "- **`step` is how far the estimate is chained from the reference.** Step 0 is",
  "  measured directly against reference cricket; step 3 is measured against",
  "  something measured against something measured against it, and the error",
  "  compounds without announcing itself.",
  "- **There is a known crossover**, pinned by `test-competition-adjust.R`: below a",
  "  certain value a weak-competition return still rates above the same return in",
  "  the reference. Whether compression should differ above and below the",
  "  competition mean was tested and is **not** established (0.137 against 0.050,",
  "  p = 0.082 — underpowered rather than refuted).",
  "- **Batting and bowling are fitted separately** and need not agree. Where they",
  "  disagree sharply that is a finding about the competition, not an error.",
  ""
)

for (bk in buckets) {
  for (role in c("batter", "bowler")) {
    lab <- sprintf("%s %s %s", toupper(bk$format), bk$gender, role)
    cli::cli_h2(lab)
    off <- tryCatch(fit_bucket(bk$format, bk$gender, role),
                    error = function(e) {
                      cli::cli_alert_warning("{lab}: {conditionMessage(e)}"); NULL
                    })
    if (is.null(off) || !nrow(off)) next

    setorder(off, -offset)
    md <- c(md, sprintf("## %s", lab), "",
            sprintf("%d competitions, %s balls, %s players.",
                    nrow(off), format(sum(off$balls), big.mark = ","),
                    format(sum(off$players), big.mark = ",")),
            "",
            "| competition | inflation vs reference (/100 balls) | assigned level (/100) | cfactor | bridges | evidence | step | balls |",
            "|---|---:|---:|---:|---:|---:|---:|---:|")
    for (i in seq_len(nrow(off))) {
      r <- off[i]
      thin <- if (is.na(r$evidence) || r$evidence < MIN_EVIDENCE_TO_SHOW) " (thin)" else ""
      md <- c(md, sprintf("| %s%s | %s | %s | %s | %s | %s | %s | %s |",
        r$comp, thin, fmt_num(100 * r$offset), fmt_num(100 * r$m_ref),
        fmt_num(r$cfactor, 3),
        ifelse(is.na(r$n_bridges), "-", as.character(r$n_bridges)),
        ifelse(is.na(r$evidence), "-", formatC(r$evidence, format = "d", big.mark = ",")),
        ifelse(is.na(r$step), "-", as.character(r$step)),
        formatC(r$balls, format = "d", big.mark = ",")))
    }
    md <- c(md, "",
            sprintf("(thin) = under %d balls of harmonic bridge evidence. Indicative only.",
                    MIN_EVIDENCE_TO_SHOW),
            "")

    # Reference competitions have no bridge evidence of their own -- they ARE
    # the anchor -- so they are kept explicitly rather than filtered as thin.
    is_ref <- off$comp %in% default_competition_reference(bk$format, bk$gender)
    top <- off[is_ref | (!is.na(evidence) & evidence >= MIN_EVIDENCE_TO_SHOW)]
    setorder(top, -offset)
    if (nrow(top) >= 2) {
      # Span the range rather than take the head. The 14 easiest competitions
      # all sit within a few runs of each other, so a top-14 matrix is a wall
      # of near-zeros; the easiest-to-hardest span is the thing worth seeing.
      h <- ceiling(MAX_MATRIX_COMPS / 2)
      keep <- if (nrow(top) <= MAX_MATRIX_COMPS) top else
        unique(rbind(head(top, h), tail(top, MAX_MATRIX_COMPS - h)))
      md <- c(md, sprintf("### %s -- pairwise inflation, runs per 100 balls", lab), "",
              sprintf("Extra runs per 100 balls the SAME player scores in the row competition versus the column one. Positive = the row is easier. Top %d by inflation, evidence >= %d or reference.",
                      nrow(keep), MIN_EVIDENCE_TO_SHOW),
              "",
              paste0("| from \\ to | ", paste(keep$comp, collapse = " | "), " |"),
              paste0("|---|", paste(rep("---:", nrow(keep)), collapse = "|"), "|"))
      for (i in seq_len(nrow(keep))) {
        cells <- vapply(seq_len(nrow(keep)), function(j) {
          if (i == j) return("-")
          fmt_num(100 * (keep$offset[i] - keep$offset[j]), 1)
        }, character(1))
        md <- c(md, paste0("| **", keep$comp[i], "** | ",
                           paste(cells, collapse = " | "), " |"))
      }
      md <- c(md, "")
    }
  }
}

dir.create(OUT_DIR, showWarnings = FALSE, recursive = TRUE)
writeLines(md, file.path(OUT_DIR, "COMPETITION-MATRIX.md"))
cli::cli_alert_success("Wrote COMPETITION-MATRIX.md ({length(md)} lines)")
