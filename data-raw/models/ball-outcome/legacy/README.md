# Legacy Models

This directory contains deprecated model implementations preserved for reference.

## BAM (Bayesian Additive Models) - DEPRECATED

**Files:**
- `model_bam_outcome_longform.R` - BAM for Test/ODI formats
- `model_bam_outcome_shortform.R` - BAM for T20 format

**Why Deprecated:**
1. **Consistency** - XGBoost is now used for both agnostic and full models
2. **Speed** - XGBoost inference is significantly faster for rating calculations
3. **SHAP compatibility** - XGBoost provides native SHAP support for interpretability
4. **Maintenance** - Single model framework reduces code complexity

**Replacement:**
- `01_train_agnostic_model.R` - XGBoost agnostic baseline
- `02_train_full_model.R` - XGBoost full model with skills

**When to Reference:**
- If investigating alternative modeling approaches
- For historical comparison of model performance
- If mgcv/BAM-specific features are needed in the future

## Key Technical Decisions

The BAM models used `mgcv::bam()` with smooth terms for continuous features. While BAM provided good calibration, the switch to XGBoost offered:

```r
# BAM approach (deprecated)
bam(outcome ~ s(over) + s(wickets) + ..., family = "multinom")

# XGBoost approach (current)
xgboost(data, nrounds, objective = "multi:softprob")
```

XGBoost advantages:
- 10x faster inference
- Native SHAP values
- Better handling of feature interactions
- Consistent with full model architecture
