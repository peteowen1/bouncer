# Flatten multi-hop chains AND resolve cycles in venue_aliases (bouncerverse#73).
#
# venue_aliases must maintain the invariant that no canonical_venue is EVER
# also an alias key -- build_venue_id_map() asserts this within a single
# call's own output, but nothing asserted it against the EXISTING table, and
# this exact bug already bit once (2026-08-29, commit d6c80c1): a
# single-hop match()-based lookup in 02_baseline_projected_score.R silently
# stopped at an intermediate name for 9 chains, still splitting real venue
# history.
#
# It bit again on 2026-09-01, in two related shapes: reconciling this
# session's #73 work (68 source-table fixes + 101 more from a fresh
# build_venue_id_map() run) created real 2-CYCLES (an existing row said
# "A -> B", this session added "B -> A", since #73's source-table fix picks
# the longest/most-qualified string while the Aug-20 batch picked whichever
# had more matches AT THE TIME) -- and separately, plain chains where the
# EXISTING table's target was a phantom string cricsheet has never used
# (0 rows), which store_venue_aliases()'s own "fix_curated" logic already
# handles for a DIRECT alias->canonical disagreement, but not for a target
# that is itself an alias pointing at a *different* phantom.
#
# Ground truth, not an arbitrary tie-break: EVERY node visited while
# resolving a chain is checked against how many rows it actually has in
# cricsheet.matches right now (mostly 0, since #73's source-table fix
# already merged the real ones), and the one with the most real rows wins --
# not just whichever the chain happens to end on.
#
# Usage: Rscript data-raw/data-acquisition/flatten_venue_aliases.R [--commit]
suppressPackageStartupMessages({
  library(DBI); library(data.table)
  devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE)
})
a <- commandArgs(trailingOnly = TRUE)
commit <- "--commit" %in% a

conn <- get_db_connection(read_only = FALSE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

va <- as.data.table(dbGetQuery(conn, "SELECT alias, canonical_venue FROM venue_aliases"))
cli::cli_alert_info("{nrow(va)} rows loaded")

# Real corpus row count for every distinct string appearing anywhere in the
# table, fetched ONCE rather than per-lookup.
all_strings <- unique(c(va$alias, va$canonical_venue))
counts_df <- as.data.table(dbGetQuery(conn, sprintf(
  "SELECT venue, COUNT(*) AS n FROM cricsheet.matches WHERE venue IN (%s) GROUP BY 1",
  paste(sprintf("'%s'", gsub("'", "''", all_strings)), collapse = ","))))
row_count <- setNames(rep(0L, length(all_strings)), all_strings)
row_count[counts_df$venue] <- counts_df$n

# Walk the chain from `v`, visiting every node (cycle-safe via `seen`), and
# return the visited node with the MOST real rows -- ties broken by whichever
# was visited first (closest to the start).
resolve_by_ground_truth <- function(v, map, counts, max_hops = 20L) {
  visited <- v
  cur <- v
  for (i in seq_len(max_hops)) {
    hit <- map[alias == cur, canonical_venue]
    if (!length(hit) || hit[1] %in% visited) break
    cur <- hit[1]
    visited <- c(visited, cur)
  }
  n <- counts[visited]
  n[is.na(n)] <- 0L
  visited[which.max(n)]
}

va[, final := vapply(alias, resolve_by_ground_truth, character(1),
                     map = va, counts = row_count)]
changed <- va[final != canonical_venue]
cli::cli_alert_info("{nrow(changed)} row{?s} need repointing to their ground-truth target")
print(changed[, .(alias, canonical_venue, final,
                  alias_rows = row_count[alias], final_rows = row_count[final])])

# A canonical_venue is never itself the alias-key of a row whose OWN final
# differs (i.e. no remaining chain) -- and a row must never point at itself
# unless it's a genuine no-op (alias == final, i.e. nothing pointed here).
final_map <- setNames(va$final, va$alias)
still_bad <- va[final %in% alias & final != alias &
                final_map[final] != final]
if (nrow(still_bad)) {
  cli::cli_abort("{nrow(still_bad)} row{?s} still chain after ground-truth resolution -- investigate.")
}

if (!commit) {
  cli::cli_alert_info("DRY RUN -- nothing written. Pass --commit to apply.")
  quit(save = "no")
}

# A node that WINS (becomes someone's final target) must not also survive as
# an alias row pointing elsewhere -- delete its own alias row if it has one
# and lost to itself... i.e. if final(node) == node, keep it (canonical,
# not an alias); if it has a row as an alias pointing to something ELSE,
# that's handled by the `changed` update below already.
for (i in seq_len(nrow(changed))) {
  dbExecute(conn, "UPDATE venue_aliases SET canonical_venue = ? WHERE alias = ?",
            params = list(changed$final[i], changed$alias[i]))
}
# Any alias whose OWN final resolves to ITSELF (it won its own chain) should
# not remain a row in venue_aliases at all -- it's now a canonical, not an
# alias of anything.
self_final <- va[final == alias, alias]
if (length(self_final)) {
  for (s in self_final) dbExecute(conn, "DELETE FROM venue_aliases WHERE alias = ?", params = list(s))
  cli::cli_alert_info("Removed {length(self_final)} row{?s} that resolved to themselves (now canonical, not an alias)")
}

va2 <- as.data.table(dbGetQuery(conn, "SELECT alias, canonical_venue FROM venue_aliases"))
still_bad2 <- va2[canonical_venue %in% alias & canonical_venue != alias]
if (nrow(still_bad2)) {
  cli::cli_abort("{nrow(still_bad2)} chain{?s} remain after the fix -- did not flatten cleanly.")
}
cli::cli_alert_success("Flattened {nrow(changed)} row{?s} by ground truth; 0 chains remain, verified against the live table ({nrow(va2)} rows).")
