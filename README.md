Introduction to Spark
================
Derek Weix
3/3/20

## Getting Started

Installations (assuming you have r, rstudio, and java).

``` r
#install.packages("sparklyr")
library("sparklyr")
```

    ## 
    ## Attaching package: 'sparklyr'

    ## The following object is masked from 'package:stats':
    ## 
    ##     filter

``` r
spark_available_versions()
```

    ##   spark
    ## 1   1.6
    ## 2   2.0
    ## 3   2.1
    ## 4   2.2
    ## 5   2.3
    ## 6   2.4
    ## 7   3.0
    ## 8   3.1
    ## 9   3.2

``` r
#spark_install(version = "3.0")
spark_installed_versions()
```

    ##   spark hadoop                                          dir
    ## 1 2.4.3    2.7 /home/ubuntu/spark/spark-2.4.3-bin-hadoop2.7
    ## 2 3.0.3    2.7 /home/ubuntu/spark/spark-3.0.3-bin-hadoop2.7
    ## 3 3.0.3    3.2 /home/ubuntu/spark/spark-3.0.3-bin-hadoop3.2
