from pyspark.sql import SparkSession
import logging
from iceberg_sql_schema_utils import *


def main():
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
    spark.sql(create_raw_battery_power_readings_schema)

    logging.info(f"""create-raw-schema -> SolarX_Raw_Transactions tables are created successfully""")


if __name__ == "__main__":
    logging.basicConfig(level = "INFO")
    main()