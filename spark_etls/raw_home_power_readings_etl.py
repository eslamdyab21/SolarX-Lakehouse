from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, TimestampType, FloatType
import logging
import sys



def read_internal_data(spark, date):
    # Read internal csv raw data
    _schema = "timestamp timestamp, min_consumption_wh float, max_consumption_wh float, avg_consumption_wh float"

    home_power_usage_df = spark.read.format("csv").schema(_schema).option("header", True)\
                    .load(f"""/home/iceberg/warehouse/home_power_usage_history/{date}.csv""")\
                    .withColumn("15_minutes_interval", F.floor((F.hour(F.col("timestamp"))*60 + F.minute(F.col("timestamp")) - 60) / 15))                                                                                                 


    spark.conf.set("spark.sql.shuffle.partitions", 92)
    logging.info(f"""raw-home-power-etl -> Read home_power_usage_history successfully as dataframe""")

    return home_power_usage_df



def read_solarx_kafka_logged_data(spark, date):
    # Read solarx kafka log raw data
    _schema = StructType([
        StructField("time_stamp", TimestampType(), True),
        StructField("current_consumption_w", FloatType(), True),
        StructField("consumption_accumulated_w", FloatType(), True),
    ])

    home_power_usage_df = spark.read.format("json").schema(_schema)\
                    .load(f"/home/iceberg/warehouse/solarx_kafka_log_data/kafka_log_home_energy_consumption_{date}.log")\
                    .withColumn("15_minutes_interval", F.floor((F.hour(F.col("time_stamp"))*60 + F.minute(F.col("time_stamp")) - 60) / 15))                                                                                                 

    spark.conf.set("spark.sql.shuffle.partitions", 92)
    logging.info(f"""raw-home-power-etl -> Read home_power_usage_history successfully as dataframe""")

    return home_power_usage_df



def load_internal_2_iceberg(home_power_usage_df):
    
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

    logging.info(f"""raw-home-power-etl -> load_internal_2_iceberg into home_power_readings iceberg table successfully""")

    

def load_kafka_2_iceberg(home_power_usage_df):
    
    # Load data into raw home_power_readings iceberg table
    home_power_usage_df.createOrReplaceTempView("temp_view")

    spark.sql("""
        INSERT INTO SolarX_Raw_Transactions.home_power_readings (timestamp, 15_minutes_interval, min_consumption_wh, max_consumption_wh)
        SELECT time_stamp                    as timestamp,
               15_minutes_interval           as 15_minutes_interval,
               current_consumption_w         as min_consumption_wh,
               current_consumption_w         as max_consumption_wh
            
        FROM temp_view
    """)

    logging.info(f"""raw-home-power-etl -> load_kafka_2_iceberg into home_power_readings iceberg table successfully""")



if __name__ == "__main__":
    logging.basicConfig(level = "INFO")

    spark = (
        SparkSession
        .builder
        .appName("create raw home power readings")
        .getOrCreate()
    )

    source_data_type = sys.argv[1]
    date = sys.argv[2]

    if source_data_type == "solarx-kafka":
        home_power_usage_df = read_solarx_kafka_logged_data(spark, date)
        load_kafka_2_iceberg(home_power_usage_df)

    elif source_data_type == "internal":
        home_power_usage_df = read_internal_data(spark, date)
        load_internal_2_iceberg(home_power_usage_df)

    else:
        logging.info(f"""raw-home-power-etl -> Please enter either solarx-kafka or internal as data_type""")
        print("Please choose either solarx-kafka or internal as data_typ")

    spark.stop()