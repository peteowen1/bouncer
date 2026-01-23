# Quick test of fast_rbind optimization
devtools::load_all()

cat("=== Testing fast_rbind() ===\n\n")

# Create test data frames
df1 <- data.frame(a = 1:3, b = letters[1:3])
df2 <- data.frame(a = 4:6, b = letters[4:6])
df3 <- data.frame(a = 7:9, b = letters[7:9])
df_list <- list(df1, df2, df3)

# Test fast_rbind
result <- bouncer:::fast_rbind(df_list)
cat("fast_rbind result:\n")
print(result)

# Check if data.table is being used
cat("\n\ndata.table available:", bouncer:::has_datatable_support(), "\n")

# Benchmark with many data frames
cat("\n=== Benchmark (1000 small data frames) ===\n")

# Create many small data frames
big_list <- lapply(1:1000, function(i) {
  data.frame(x = runif(10), y = runif(10), z = sample(letters, 10, replace = TRUE))
})

# Time both methods
t_rbind <- system.time(do.call(rbind, big_list))["elapsed"]
t_fast <- system.time(bouncer:::fast_rbind(big_list))["elapsed"]

cat("do.call(rbind):", round(t_rbind, 3), "sec\n")
cat("fast_rbind:    ", round(t_fast, 3), "sec\n")
cat("Speedup:       ", round(t_rbind / t_fast, 1), "x\n")
