# This chapter will focus on using familiar R packages to perform
# data analysis in Spark.

library("sparklyr")
library("dplyr")


sc <- spark_connect(master = "local", spark_home = "/home/ubuntu/spark/spark-3.0.3-bin-hadoop3.2")

cars <- copy_to(sc, mtcars)

summarize_all(cars, mean)

summarize_all(cars, mean) |>
  show_query()

cars |>
  mutate(transmission = ifelse(am == 0, "automatic", "manual")) |>
  group_by(transmission) |>
  summarise_all(mean)

# Built in functions, percentile is a Spark SQL function, not an R function.
# We can't use native R functions here because they aren't dplyr, but we can 
# adopt the Spark SQL functions instead.

# Spark SQL
summarise(cars, mpg_percentile = percentile(mpg, array(0.25, .5, .75))) |>
  mutate(mpg_percentile = explode(mpg_percentile))

# Native R
summarise(cars, mpg_percentile = quantile(mpg, c(0.25, .5, .75)))

# Correlations
ml_corr(cars)

library("corrr")
correlate(cars, use = "pairwise.complete.obs", method = "pearson") |>
  shave() |>
  rplot()

# ggplot2
library("ggplot2")
ggplot(aes(as.factor(cyl), mpg), data = mtcars) + geom_col()

# We want to transform our data in spark, and bring back the results
# into R. If we are careless we could bring the entire dataset down into
# R, which could crash our system if it is too big.

car_group <- cars |>
  group_by(cyl) |>
  summarise(mpg = sum(mpg, na.rm = TRUE)) |>
  collect() |>
  print()

ggplot(aes(as.factor(cyl), mpg), data = car_group) +
  geom_col()

# dbplot
library("dbplot")

cars |>
  dbplot_histogram(mpg, binwidth = 3) +
  labs(title = "MPG Distribution",
       subtitle = "Histogram over miles per gallon")

# scatter plots
# These can't work by `pushing the compution` to Spark. Every observation
# is a single point, so we would need to load the whole dataframe into R.
# An easy alternative is the raster plot. A grid of x/y positions and the 
# count. This can limit the information which needs to be loaded into R.
ggplot(aes(mpg, wt), data = mtcars) +
  geom_point()

dbplot_raster(cars, mpg, wt, resolution = 16) +
  labs(title = "2D Histogram")

# Models
cars |>
  ml_linear_regression(mpg ~ .) |>
  summary()

cars |>
  ml_generalized_linear_regression(mpg ~ hp + cyl) |>
  summary()

# Cached Data
# Doing computations on giant data sets can be slow. Saving data after it has 
# been transformed can help save time if that form will be repeated used. The 
# compute command can take a dplyr pipe and save the results to Spark's memory

cached_cars <- cars |>
  mutate(cyl = paste0("cyl_", cyl)) |>
  compute("cached_cars")

# End session (continued in Rmarkdown)
spark_disconnect(sc)

