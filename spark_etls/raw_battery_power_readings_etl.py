from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, TimestampType, FloatType, IntegerType, StringType, MapType
import logging
import sys



def read_solarx_kafka_logged_data(spark, date):
    # Read solarx kafka log raw data
    battery_schema = StructType([
        StructField("capacity_kwh", FloatType(), True),
        StructField("max_charge_speed_w", FloatType(), True),
        StructField("current_energy_wh", FloatType(), True),
        StructField("is_charging", IntegerType(), True),
        StructField("status", StringType(), True),
        StructField("max_output_w", FloatType(), True)
    ])

    _schema = StructType([
        StructField("time_stamp", TimestampType(), True),
        StructField("batteries", MapType(StringType(), battery_schema), True),  # Dictionary of batteries
    ])

    battery_readings_df = spark.read.format("json").schema(_schema)\
                    .load(f"/home/iceberg/warehouse/solarx_kafka_log_data/kafka_log_battery_data_{date}.log")\
                    .select("time_stamp", F.explode("batteries").alias("battery_name", "battery_data"))\
                    .select(
                            F.col("time_stamp"),
                            F.col("battery_name"),
                            F.col("battery_data.capacity_kwh").alias("capacity_kwh"),
                            F.col("battery_data.max_charge_speed_w").alias("max_charge_speed_w"),
                            F.col("battery_data.current_energy_wh").alias("current_energy_wh"),
                            F.col("battery_data.is_charging").alias("is_charging"),
                            F.col("battery_data.status").alias("status"),
                            F.col("battery_data.max_output_w").alias("max_output_w")
                    ).withColumn("15_minutes_interval", F.floor((F.hour(F.col("time_stamp"))*60 + F.minute(F.col("time_stamp")) - 60) / 15))                                                                                            


    logging.info(f"""raw-battery-power-readings-etl -> Read battery_readings logs successfully""")
    return battery_readings_df




def load_2_iceberg(spark, battery_readings_df):
    battery_readings_df.createOrReplaceTempView("temp_view")
    spark.sql(f"""
        INSERT INTO SolarX_Raw_Transactions.battery_readings (timestamp, 15_minutes_interval, battery_name, capacity_kwh,
                                                          max_charge_speed_w, current_energy_wh, is_charging, status, max_output_w)
        SELECT time_stamp              as timestamp,
            15_minutes_interval        as 15_minutes_interval,
            battery_name               as battery_name,
            capacity_kwh               as capacity_kwh,
            max_charge_speed_w         as max_charge_speed_w,
            current_energy_wh          as current_energy_wh,
            is_charging                as is_charging,
            status                     as status,
            max_output_w               as max_output_w
            
        FROM temp_view
    """)

    logging.info(f"""raw-battery-power-readings-etl -> Load into battery_readings iceberg table successfully""")


    

if __name__ == "__main__":
    logging.basicConfig(level = "INFO")

    spark = (
        SparkSession
        .builder
        .appName("load raw battery power readings")
        .getOrCreate()
    )

    source_data_type = sys.argv[1]
    date = sys.argv[2]
    
    if source_data_type == "solarx-kafka":
        battery_readings_df = read_solarx_kafka_logged_data(spark, date)

        load_2_iceberg(spark, battery_readings_df)

    else:
        logging.info(f"""raw-battery-power-readings-etl -> Please enter either solarx-kafka or internal as data_type""")
        print("Please choose either solarx-kafka or internal as data_type")


    spark.stop()