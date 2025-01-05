from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import logging
import sys
from iceberg_sql_schema_utils import *


def main(date):
    spark = (
        SparkSession
        .builder
        .appName("create raw home power readings")
        .getOrCreate()
    )

    # Read raw data
    _schema = "timestamp timestamp, min_consumption_wh float, max_consumption_wh float, avg_consumption_wh float"

    home_power_usage_df = spark.read.format("csv").schema(_schema).option("header", True)\
                    .load(f"""/home/iceberg/warehouse/home_power_usage_history/{date}.csv""")\
                    .withColumn("15_minutes_interval", F.floor((F.hour(F.col("timestamp"))*60 + F.minute(F.col("timestamp")) - 60) / 15))                                                                                                 


    spark.conf.set("spark.sql.shuffle.partitions", 92)
    logging.info(f"""raw-home-power-etl -> Read home_power_usage_history successfully as dataframe""")


    # Load data into raw home_power_readings iceberg table
    home_power_usage_df.createOrReplaceTempView("temp_view")

    spark.sql("""
        INSERT INTO SolarX_Raw_Transactions.home_power_readings (timestamp, 15_minutes_interval, min_consumption_wh, max_consumption_wh)
        SELECT timestamp                  as timestamp,
               15_minutes_interval        as 15_minutes_interval,
               min_consumption_wh         as min_consumption_wh,
               max_consumption_wh         as max_consumption_wh
            
        FROM temp_view
    """)

    logging.info(f"""raw-home-power-etl -> Load into home_power_readings iceberg table successfully""")



if __name__ == "__main__":
    date = sys.argv[1]
    logging.basicConfig(level = "INFO")
    main(date)