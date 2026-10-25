# Vignette code guard
#
# Every vignette sets `eval = FALSE`, so pkgdown publishes code that never
# runs. By 2026-10 about 20 calls in them used arguments the functions do not
# have (e.g. `simulate_season(event =, season =, n_sims =)` against
# `simulate_season(fixtures, ...)`) and one called a function that did not
# exist (`predict_matchup_outcome()`). This test parses each vignette's R
# chunks without running them and checks every call to a bouncer function:
# the function must exist, and every named argument must be one of its formals.
#
# Names must match exactly. R would partial-match `team1 =` onto
# `team1_skill`, so `quick_match_simulation(team1 = "India")` runs but passes a
# team name as a skill number; an exact check catches that too.

vignette_dir <- function() {
  # Source tree (devtools::test) has vignettes two levels up from tests/testthat.
  test_path("..", "..", "vignettes")
}

vignette_code <- function(path) {
  lines <- readLines(path, warn = FALSE)
  out <- character()
  inside <- FALSE
  for (x in lines) {
    if (!inside && startsWith(x, "```{r")) {
      inside <- TRUE
      next
    }
    if (inside && startsWith(x, "```")) {
      inside <- FALSE
      next
    }
    if (inside) out <- c(out, x)
  }
  out
}

# Names assigned anywhere in the code (`x <- ...`, `x = ...` at top level,
# function arguments), so local helpers are not mistaken for missing functions.
assigned_names <- function(exprs) {
  found <- character()
  walk <- function(x) {
    if (is.call(x)) {
      op <- x[[1]]
      if (is.name(op) && as.character(op) %in% c("<-", "=", "<<-") &&
          is.name(x[[2]])) {
        found <<- c(found, as.character(x[[2]]))
      }
      for (a in as.list(x)[-1]) if (!missing(a)) walk(a)
    }
  }
  for (e in exprs) walk(e)
  unique(found)
}

# Returns a character vector of problems found in one vignette.
check_vignette_calls <- function(path, ns = asNamespace("bouncer")) {
  exprs <- parse(text = vignette_code(path), keep.source = FALSE)
  local_names <- assigned_names(exprs)
  problems <- character()
  file <- basename(path)

  walk <- function(x) {
    if (!is.call(x)) return(invisible())
    fn <- x[[1]]
    if (is.name(fn)) {
      nm <- as.character(fn)
      if (!nm %in% local_names) {
        if (!exists(nm, envir = ns)) {
          problems <<- c(problems, sprintf("%s: %s() does not exist", file, nm))
        } else if (exists(nm, envir = ns, inherits = FALSE)) {
          f <- get(nm, envir = ns)
          if (is.function(f) && !is.primitive(f)) {
            formal_names <- names(formals(f))
            arg_names <- names(as.list(x))[-1]
            arg_names <- arg_names[!is.na(arg_names) & nzchar(arg_names)]
            if (!"..." %in% formal_names) {
              bad <- setdiff(arg_names, formal_names)
              if (length(bad) > 0) {
                problems <<- c(problems, sprintf(
                  "%s: %s() has no argument(s) %s", file, nm,
                  paste(bad, collapse = ", ")
                ))
              }
            }
          }
        }
      }
    }
    # `pkg::f()` and `x$f()` have a call, not a name, in the function
    # position, so they are skipped above; nested calls in arguments are
    # checked here.
    for (a in as.list(x)[-1]) if (!missing(a)) walk(a)
    invisible()
  }

  for (e in exprs) walk(e)
  problems
}

test_that("vignette code calls bouncer functions with arguments they have", {
  dir <- vignette_dir()
  skip_if_not(dir.exists(dir), "vignettes/ not available (installed-package test run)")
  files <- list.files(dir, pattern = "[.]Rmd$", full.names = TRUE)
  expect_gt(length(files), 0)

  problems <- unlist(lapply(files, check_vignette_calls))
  expect(
    length(problems) == 0,
    paste0("Vignette calls that would fail or misbehave:\n",
           paste0("  ", problems, collapse = "\n"))
  )
})

test_that("the vignette checker flags a wrong argument and a missing function", {
  tmp <- tempfile(fileext = ".Rmd")
  on.exit(unlink(tmp), add = TRUE)
  writeLines(c(
    "```{r}",
    "x <- simulate_season(event = \"IPL\", n_sims = 10)",
    "y <- predict_matchup_outcome(batter = \"A\")",
    "z <- simulate_season(x)",
    "```"
  ), tmp)
  problems <- check_vignette_calls(tmp)
  expect_length(problems, 2)
  expect_match(problems[1], "simulate_season\\(\\) has no argument\\(s\\) event, n_sims")
  expect_match(problems[2], "predict_matchup_outcome\\(\\) does not exist")
})
