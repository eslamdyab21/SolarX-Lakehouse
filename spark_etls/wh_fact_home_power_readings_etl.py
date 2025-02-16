from pyspark.sql import SparkSession
import logging
import sys


def load_2_iceberg(spark, date):

    spark.sql(f"""
        WITH staging_table AS (
            SELECT
                TIMESTAMP(FLOOR(UNIX_MICROS(timestamp) / (15 * 60 * 1000000)) * (15 * 60)) AS home_power_reading_key,
                DATE(timestamp) AS date,
                15_minutes_interval,
                SUM(min_consumption_wh) AS min_consumption_power_wh,
                SUM(max_consumption_wh) AS max_consumption_power_wh
            FROM 
                SolarX_Raw_Transactions.home_power_readings
            WHERE 
                DATE(timestamp) =  DATE('{date}')
            GROUP BY 
                15_minutes_interval, home_power_reading_key, DATE(timestamp)
        )


            
        MERGE INTO SolarX_WH.fact_home_power_readings AS target
        USING staging_table AS source
        ON target.home_power_reading_key = source.home_power_reading_key
            
        WHEN NOT MATCHED THEN
            INSERT (home_power_reading_key, 
                    home_key, 
                    date_key, 
                    min_consumption_power_wh,
                    max_consumption_power_wh
            
            ) 
            VALUES (source.home_power_reading_key,
                    (SELECT home_key FROM SolarX_WH.dim_home WHERE dim_home.current_flag = TRUE), 
                    source.home_power_reading_key,
                    source.min_consumption_power_wh,
                    source.max_consumption_power_wh     
            );
    """)

    logging.info(f"""wh_fact_home_power_readings_etl -> Load into fact_home_power_readings iceberg table successfully""")

    

if __name__ == "__main__":
    logging.basicConfig(level = "INFO")
    date = sys.argv[1]

    spark = (
        SparkSession
        .builder
        .appName("load wh fact home power readings")
        .getOrCreate()
    )

    load_2_iceberg(spark, date)

    spark.stop()
