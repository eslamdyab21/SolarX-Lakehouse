from pyspark.sql import SparkSession
from iceberg_sql_schema_utils import *


spark = (
    SparkSession
    .builder
    .appName("create raw schema")
    .getOrCreate()
)


# -------------- SolarX_Raw_Transactions -------------
spark.sql(create_raw_namespace)
spark.sql(create_raw_home_power_readings_schema)
spark.sql(create_raw_solar_panel_schema)
spark.sql(create_raw_solar_panel_power_readings_schema)



# -------------- SolarX_WH -------------
spark.sql(create_wh_namespace)
spark.sql(create_wh_dim_home_schema)
spark.sql(create_wh_dim_home_appliances_schema)
spark.sql(create_wh_fact_home_power_readings_schema)
spark.sql(create_wh_dim_solar_panel_schema)
spark.sql(create_wh_fact_solar_panel_power_readings_schema)
spark.sql(create_wh_dim_date_schema)
