from pyspark.sql import SparkSession
import logging
from iceberg_sql_schema_utils import *


def main():
    spark = (
        SparkSession
        .builder
        .appName("create wh schema")
        .getOrCreate()
    )

    # -------------- SolarX_WH -------------
    spark.sql(create_wh_namespace)
    spark.sql(create_wh_dim_home_schema)

    spark.sql(create_wh_dim_home_appliances_schema)
    spark.sql(create_wh_fact_home_power_readings_schema)

    spark.sql(create_wh_dim_solar_panel_schema)
    spark.sql(create_wh_fact_solar_panel_power_readings_schema)

    spark.sql(create_wh_dim_solar_panel_schema)
    spark.sql(create_wh_fact_solar_panel_power_readings_schema)

    spark.sql(create_wh_dim_battery_schema)
    spark.sql(create_wh_fact_battery_power_readings_schema)

    spark.sql(create_wh_dim_date_schema)

    logging.info(f"""create-wh-schema -> SolarX_WH tables are created successfully""")



if __name__ == "__main__":
    logging.basicConfig(level = "INFO")
    main()