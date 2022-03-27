# MLlib (machine learning library) is a library with many 
# different model types available.

# linear regression, decision trees, gradient boosted trees,
# k-means clustering, ...

# Check the appendix for all available functions. We can do modeling in 
# spark via R in a natural way, using these functions.

# Download OkCupid Data
download.file(
  "https://github.com/r-spark/okcupid/raw/master/profiles.csv.zip",
  "okcupid.zip"
)

unzip("okcupid.zip", exdir = "data")
unlink("okcupid.zip")

# Read in data & sample data if it is too big.
profiles <- read.csv("data/profiles.csv")
write.csv(dplyr::sample_n(profiles, 10^2),
          "data/profiles_reduced.csv", row.names = FALSE)

### Begin Data Analysis
library("sparklyr")
library("ggplot2")
theme_set(theme_classic())
library("dbplot")
library("dplyr")

sc <- spark_connect(master = "local", spark_home = "/home/ubuntu/spark/spark-3.0.3-bin-hadoop3.2")

# Create Data Frame in Spark
okc <- spark_read_csv(
  sc,
  "data/profiles.csv",
  escape = "\"", 
  memory = FALSE,
  options = list(multiline = TRUE)
  ) |> 
  mutate(height = as.numeric(height),
         income = ifelse(income == -1, NA, as.numeric(income))
  ) |> 
  mutate(sex = ifelse(is.na(sex), "missing", sex)) |> 
  mutate(drinks = ifelse(is.na(drinks), "missing", drinks)) |> 
  mutate(drugs = ifelse(is.na(drugs), "missing", drugs)) |> 
  mutate(job = ifelse(is.na(job), "missing", job))

glimpse(okc)

# Add Response Variable
okc <- okc |> 
  mutate(not_working = ifelse(job %in% c("student", "unemployed", "retired"), 1, 0))

okc |> 
  group_by(not_working) |> 
  tally()

# Split Data
data_splits <- sdf_random_split(okc, training = 0.8, testing = 0.2, seed = 42)
okc_train <- data_splits$training
okc_test <- data_splits$testing

# Describe Data 
okc_train |> 
  group_by(not_working) |> 
  tally() |> 
  mutate(frac = n/sum(n))

sdf_describe(okc_train, cols = c("age", "income"))

dbplot_histogram(okc_train, age)

# Proportion Unemployed by Religion
prop_data <- okc_train |> 
  mutate(religion = regexp_extract(religion, "^\\\\w+", 0)) |> 
  group_by(religion, not_working) |> 
  tally() |> 
  group_by(religion) |> 
  summarize(
    count = sum(n),
    prop = sum(not_working*n)/sum(n)
  ) |> 
  mutate(se = sqrt(prop *(1-prop)/count)) |> 
  collect()

prop_data |> 
  ggplot(aes(x = religion, y = prop)) +
  geom_point(size = 2) +
  geom_errorbar(aes(ymin = prop-1.96*se,
                    ymax = prop+1.96*se), width = 0.1) +
  geom_hline(yintercept = sum(prop_data$prop * prop_data$count)/ sum(prop_data$count),
             linetype = "dashed", color = "grey")

# Contingency Plot
library("ggmosaic")
library("forcats")
library("tidyr")

contingency_tbl <- okc_train |> 
  sdf_crosstab("drinks", "drugs") |> 
  collect()

contingency_tbl |> 
  rename(drinks = drinks_drugs) |> 
  gather("drugs", "count", missing:sometimes) |> 
  mutate(
    drinks = as_factor(drinks) |> 
      fct_relevel("missing", "not at all", "rarely", "socially",
                  "often", "very often", "desperately"),
    drugs = as_factor(drugs) |> 
      fct_relevel("missing", "never", "sometimes", "often")
  ) |> 
  ggplot() +
  geom_mosaic(aes(x = product(drinks, drugs), fill = drinks, weight = count))

# Feature Engineering
# Scaling Age
scale_values <- okc_train |> 
  summarize(
    mean_age = mean(age),
    sd_age = sd(age)
  ) |> 
  collect()

okc_train <- okc_train |> 
  mutate(scaled_age = (age - !!scale_values$mean_age)/!!scale_values$sd_age)

dbplot_histogram(okc_train, scaled_age)

# Create Dummy Variables for Race
okc_train |> 
  group_by(ethnicity) |> 
  tally()

ethnicities <- c("asian", "middle eastern", "black", "native american", "indian",
                 "pacific islander", "hispanic / latin", "white", "other")

ethnicity_vars <- ethnicities %>%
  purrr::map(~ expr(ifelse(like(ethnicity, !!.x), 1, 0))) %>%
  purrr::set_names(paste0("ethnicity_", gsub("\\s|/", "", ethnicities)))

okc_train <- mutate(okc_train, !!!ethnicity_vars)

okc_train |> 
  select(starts_with("ethnicity_")) |> 
  glimpse()

# Extract Total Number of Characters from all 10 Essay
okc_train <- okc_train |> 
  mutate(
    essay_length = char_length(paste(!!!syms(paste0("essay", 0:9))))
  ) |> compute()

dbplot_histogram(okc_train, essay_length, bins = 100)

# Supervised Learning
# Cross Validation Sets
vfolds <- sdf_random_split(
  okc_train,
  weights = purrr::set_names(rep(0.1, 10), paste0("fold", 1:10)),
  seed = 42
)

# Analysis/Assessment Sets
analysis_set <- do.call(rbind, vfolds[2:10])
assessment_set <- vfolds[[1]]

# Scale Age
make_scale_age <- function(analysis_data) {
  scale_values <- analysis_data |> 
    summarize(
      mean_age = mean(age),
      sd_age = sd(age)
    ) |> 
    collect()
  
  function(data) {
    data |> 
      mutate(scaled_age = (age - !!scale_values$mean_age)/!!scale_values$sd_age)
  }
}

scale_age <- make_scale_age(analysis_set)
train_set <- scale_age(analysis_set)
validation_set <- scale_age(assessment_set)

# Logistic Regression
lr <- ml_logistic_regression(
  analysis_set, not_working ~ scaled_age + sex + drinks + drugs + essay_length
)

validation_summary <- ml_evaluate(lr, assessment_set)

# ROC Curve
roc <- validation_summary$roc() |> 
  collect()

ggplot(roc, aes(FPR, TPR)) +
  geom_line() + geom_abline(lty = "dashed")

validation_summary$area_under_roc()

# Cross Validation
cv_results <- purrr::map_df(1:10, function(v) {
  analysis_set <- do.call(rbind, vfolds[setdiff(1:10, v)]) %>% compute()
  assessment_set <- vfolds[[v]]
  
  scale_age <- make_scale_age(analysis_set)
  train_set <- scale_age(analysis_set)
  validation_set <- scale_age(assessment_set)
  
  model <- ml_logistic_regression(
    analysis_set, not_working ~ scaled_age + sex + drinks + drugs + essay_length
  )
  s <- ml_evaluate(model, assessment_set)
  roc_df <- s$roc() %>% 
    collect()
  auc <- s$area_under_roc()
  
  tibble(
    Resample = paste0("Fold", stringr::str_pad(v, width = 2, pad = "0")),
    roc_df = list(roc_df),
    auc = auc
  )
})
