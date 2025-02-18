from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, TimestampType, FloatType
import logging
import sys



def read_solarx_kafka_logged_data(spark, date):
    # Read solarx kafka log raw data
    _schema = StructType([
        StructField("time_stamp", TimestampType(), True),
        StructField("current_consumption_w", FloatType(), True),
        StructField("consumption_accumulated_w", FloatType(), True),
    ])

    solar_panel_readings_df = spark.read.format("json").schema(_schema)\
                    .load(f"/home/iceberg/warehouse/solarx_kafka_log_data/kafka_log_solar_energy_data_{date}.log")\
                    .withColumn("15_min_interval", F.floor((F.hour(F.col("time_stamp"))*60 + F.minute(F.col("time_stamp")) - 60) / 15))                                                                                                 


    return solar_panel_readings_df



def read_internal_data(spark, date):
    # Read internal raw data
    _schema = "timestamp timestamp, solar_intensity float, temp float"

    weather_df = spark.read.format("csv").schema(_schema).option("header", True)\
                    .load(f"""/home/iceberg/warehouse/weather_history_splitted_resampled/{date}.csv""")\
                    .withColumn("15_min_interval", F.floor((F.hour(F.col("timestamp"))*60 + F.minute(F.col("timestamp")) - 60) / 15))                                                                                                 


    spark.conf.set("spark.sql.shuffle.partitions", 92)
    logging.info(f"""raw-solar-panel-power-readings-etl -> Read weather_history_splitted_resampled successfully as dataframe""")


    return weather_df



def calc_solar_readings(spark, panel_id, weather_df):
    # Loading solar_panel table
    solar_panel_df = spark.read.format("iceberg").load("SolarX_Raw_Transactions.solar_panel")

    # Querying panel power ratings
    solar_panel_rating = solar_panel_df.filter(solar_panel_df.id == panel_id) \
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



def load_internal_2_iceberg(spark, solar_panel_readings_df, panel_id):
    solar_panel_readings_df.createOrReplaceTempView("temp_view")
    spark.sql(f"""
        INSERT INTO SolarX_Raw_Transactions.solar_panel_readings (timestamp, 15_minutes_interval, panel_id, generation_power_wh)
        SELECT timestamp                  as timestamp,
               15_min_interval            as 15_minutes_interval,
               {panel_id}                 as panel_id,
               current_generation_watt    as generation_power_wh
            
        FROM temp_view
    """)

    logging.info(f"""raw-solar-panel-power-readings-etl -> panel: {panel_id} -> load_internal_2_iceberg into solar_panel_readings iceberg table successfully""")



def load_kafka_2_iceberg(spark, solar_panel_readings_df, panel_id):
    solar_panel_readings_df.createOrReplaceTempView("temp_view")
    spark.sql(f"""
        INSERT INTO SolarX_Raw_Transactions.solar_panel_readings (timestamp, 15_minutes_interval, panel_id, generation_power_wh)
        SELECT time_stamp                 as timestamp,
               15_min_interval            as 15_minutes_interval,
               {panel_id}                 as panel_id,
               current_consumption_w      as generation_power_wh
            
        FROM temp_view
    """)

    logging.info(f"""raw-solar-panel-power-readings-etl -> panel: {panel_id} -> load_kafka_2_iceberg into solar_panel_readings iceberg table successfully""")



if __name__ == "__main__":
    logging.basicConfig(level = "INFO")

    spark = (
        SparkSession
        .builder
        .appName("create raw solar panel power readings")
        .getOrCreate()
    )

    source_data_type = sys.argv[1]
    date = sys.argv[2]
    
    if source_data_type == "solarx-kafka":
        solar_panel_readings_df = read_solarx_kafka_logged_data(spark, date)

        panel_id = 4
        load_kafka_2_iceberg(spark, solar_panel_readings_df, panel_id)

    elif source_data_type == "internal":
        weather_df = read_internal_data(spark, date)

        panel_id = 1
        solar_panel_readings_df = calc_solar_readings(spark, panel_id, weather_df)
        load_internal_2_iceberg(spark, solar_panel_readings_df, panel_id)

        panel_id = 2
        solar_panel_readings_df = calc_solar_readings(spark, panel_id, weather_df)
        load_internal_2_iceberg(spark, solar_panel_readings_df, panel_id)

        panel_id = 3
        solar_panel_readings_df = calc_solar_readings(spark, panel_id, weather_df)
        load_internal_2_iceberg(spark, solar_panel_readings_df, panel_id)

    else:
        logging.info(f"""raw-solar-panel-power-readings-etl -> Please enter either solarx-kafka or internal as data_type""")
        print("Please choose either solarx-kafka or internal as data_typ")


    spark.stop()