from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import logging
import sys
from iceberg_sql_schema_utils import *


def calc_solar_readings(spark, panel_id, weather_df):
    # Loading solar_panel table
    solar_panele_df = spark.read.format("iceberg").load("SolarX_Raw_Transactions.solar_panel")

    # Quering panel power ratings
    solar_panel_rating = solar_panele_df.filter(solar_panele_df.id == panel_id) \
                                    .select("capacity_kwh", "intensity_power_rating", "temperature_power_rating") \
                                    .first()

    solar_panel_rating_kwh       = solar_panel_rating["capacity_kwh"]
    solar_intensity_power_rating = solar_panel_rating["intensity_power_rating"]
    temp_power_rating            = solar_panel_rating["temperature_power_rating"]
    
    solar_panel_rating_w_5ms = solar_panel_rating_kwh*1000/(60*60*1000/5)


    # Calculating the power value into a new df
    solar_panel_readings_df = weather_df.withColumn('current_generation_watt', solar_panel_rating_w_5ms \
                * (1 /(1 - (temp_power_rating - F.col("temp"))/(temp_power_rating))) \
                * (1 - (solar_intensity_power_rating - F.col("solar_intensity"))/solar_intensity_power_rating) \
                                                           ).drop("solar_intensity", "temp")


    logging.info(f"""raw-solar-panel-power-readings-etl -> panel: {panel_id} -> Calculated the power successfully""")

    return solar_panel_readings_df



def insert_data2iceberg(spark, solar_panel_readings_df, panel_id):
    solar_panel_readings_df.createOrReplaceTempView("temp_view")
    spark.sql(f"""
        INSERT INTO SolarX_Raw_Transactions.solar_panel_readings (timestamp, 15_minutes_interval, panel_id, generation_power_wh)
        SELECT timestamp                  as timestamp,
               15_min_interval            as 15_minutes_interval,
               {panel_id}                 as panel_id,
               current_generation_watt    as generation_power_wh
            
        FROM temp_view
    """)

    logging.info(f"""raw-solar-panel-power-readings-etl -> panel: {panel_id} -> Load into solar_panel_readings iceberg table successfully""")



def main(date):
    spark = (
        SparkSession
        .builder
        .appName("create raw solar panel power readings")
        .getOrCreate()
    )

    # Read raw data
    _schema = "timestamp timestamp, solar_intensity float, temp float"

    weather_df = spark.read.format("csv").schema(_schema).option("header", True)\
                    .load(f"""/home/iceberg/warehouse/weather_history_splitted_resampled/{date}.csv""")\
                    .withColumn("15_min_interval", F.floor((F.hour(F.col("timestamp"))*60 + F.minute(F.col("timestamp")) - 60) / 15))                                                                                                 


    spark.conf.set("spark.sql.shuffle.partitions", 92)
    logging.info(f"""raw-solar-panel-power-readings-etl -> Read weather_history_splitted_resampled successfully as dataframe""")


    panel_id = 1
    solar_panel_readings_df = calc_solar_readings(spark, panel_id, weather_df)
    insert_data2iceberg(spark, solar_panel_readings_df, panel_id)

    panel_id = 2
    solar_panel_readings_df = calc_solar_readings(spark, panel_id, weather_df)
    insert_data2iceberg(spark, solar_panel_readings_df, panel_id)

    panel_id = 3
    solar_panel_readings_df = calc_solar_readings(spark, panel_id, weather_df)
    insert_data2iceberg(spark, solar_panel_readings_df, panel_id)



if __name__ == "__main__":
    date = sys.argv[1]
    logging.basicConfig(level = "INFO")
    main(date)