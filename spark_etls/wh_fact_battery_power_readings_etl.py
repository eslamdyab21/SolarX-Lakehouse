from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
import logging
import sys


staging_query = """
    WITH battery_readings_15m AS (
        SELECT 
            TIMESTAMP(FLOOR(UNIX_MICROS(timestamp) / (15 * 60 * 1000000)) * (15 * 60)) AS timestamp_15min,
            CAST(SPLIT_PART(battery_name, '_', 2) AS INT) as battery_id,
            15_minutes_interval,
            current_energy_wh,
            is_charging,
            status,
            ROW_NUMBER() OVER (PARTITION BY TIMESTAMP(FLOOR(UNIX_MICROS(timestamp) / (15 * 60 * 1000000)) * (15 * 60)), battery_name  ORDER BY timestamp DESC) AS row_num
        FROM SolarX_Raw_Transactions.battery_readings
    )

    SELECT * FROM battery_readings_15m
    WHERE row_num = 1
"""

dim_battery_current_query = """
    SELECT 
        battery_key,	
        battery_id as dim_battery_id
    FROM 
        SolarX_WH.dim_battery
    WHERE 
        dim_battery.current_flag = TRUE
"""

def broadcast_join(spark):
    staging_df = spark.sql(staging_query)
    dimension_df = spark.sql(dim_battery_current_query)

    # Broadcast the smaller dimension table for the join
    joined_df = staging_df.join(
        broadcast(dimension_df),
        (staging_df.battery_id == dimension_df.dim_battery_id),
        "left"
    )


    logging.info(f"""wh_fact_battery_power_readings_etl -> Broadcast join successfully""")
    return joined_df



def load_2_iceberg(joined_df):

    joined_df.createOrReplaceTempView("staging_temp_view")

    spark.sql(f"""
        MERGE INTO SolarX_WH.fact_battery_power_readings AS target
        USING staging_temp_view AS source
        ON target.battery_id = source.battery_id AND target.date_key = source.timestamp_15min
            
        WHEN NOT MATCHED THEN
            INSERT (battery_key, 
                    date_key, 
                    battery_id,
                    current_energy_wh,
                    is_charging,
                    status    
            ) 
            VALUES (source.battery_key, 
                    source.timestamp_15min,
                    source.battery_id,
                    source.current_energy_wh,
                    source.is_charging,  
                    source.status
            );
    """)

    logging.info(f"""wh_fact_battery_power_readings_etl -> Load into fact_battery_power_readings iceberg table successfully""")

    

if __name__ == "__main__":
    logging.basicConfig(level = "INFO")
    date = sys.argv[1]

    spark = (
        SparkSession
        .builder
        .appName("load wh fact battery power readings")
        .getOrCreate()
    )

    joined_df = broadcast_join(spark)
    load_2_iceberg(joined_df)

    spark.stop()
