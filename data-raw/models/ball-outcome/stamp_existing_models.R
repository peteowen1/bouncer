# One-off migration for bouncerverse#50: stamp the known-good 2026-08-18 agnostic
# models with the build date the loaders now require. The date is their file
# mtime, which is the training run -- these are the artefacts the leak fix
# produced. Verified by comparing the full tree dump before and after.
suppressMessages(library(xgboost))
md <- "C:/dev/bouncerverse/bouncerdata/models"
ok <- TRUE
for (f in c("agnostic_outcome_t20.ubj", "agnostic_outcome_odi.ubj", "agnostic_outcome_test.ubj")) {
  p <- file.path(md, f)
  built <- format(as.Date(file.mtime(p)), "%Y-%m-%d")
  m <- xgb.load(p)
  before <- xgb.dump(m)
  nf_before <- xgb.config(m)$learner$learner_model_param$num_feature
  xgb.attr(m, "bouncer_build_date") <- built
  xgb.save(m, p)
  m2 <- xgb.load(p)
  same <- identical(before, xgb.dump(m2))
  nf_same <- identical(nf_before, xgb.config(m2)$learner$learner_model_param$num_feature)
  stamp <- xgb.attr(m2, "bouncer_build_date")
  ok <- ok && same && nf_same && identical(stamp, built)
  cat(sprintf("%-28s built %s | trees identical %-5s | num_feature %s kept %-5s | stamp %s\n",
              f, built, same, nf_before, nf_same, stamp))
}
cat("\nALL CHECKS PASS:", ok, "\n")
