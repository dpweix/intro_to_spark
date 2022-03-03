### Installation #############################################################

#install.packages("sparklyr")
library("sparklyr")
spark_available_versions()
#spark_install(version = "3.0")
spark_installed_versions()

### Usage ####################################################################
# Create Local Cluster
sc <- spark_connect(master = "local", version = "3.0.3")

# copy mtcars into Apache spark
cars <- copy_to(sc, mtcars)

# View cluster sc
cars
spark_web(sc)

# Using SQL via DBI package or 
# use dplyr (dplyr is probably better for an R user)
library("dplyr")

# Note that dply verbs work... but base R won't.
count(cars) # nrow(cars)

# Analysis
select(cars, hp, mpg) |> 
  slice_sample(n = 100) |> 
  collect() |> 
  plot()

# Modeling
model <- ml_linear_regression(cars, mpg ~ hp)

model |> 
  ml_predict(copy_to(sc, tibble(hp = 250 + 10*1:10))) |> 
  transmute(hp = hp, mpg = prediction) |> 
  full_join(select(cars, hp, mpg)) |> 
  collect() |> 
  plot()

# Read/Write
spark_write_csv(cars, "cars.csv")
cars <- spark_read_csv(sc, "cars.csv")

# Extensions
#install.packages("sparklyr.nested")
sparklyr.nested::sdf_nest(cars, hp) |> 
