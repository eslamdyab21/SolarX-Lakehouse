from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import logging
import sys


def main(date):
    spark = (
        SparkSession
        .builder
        .appName("load wh fact home power readings")
        .getOrCreate()
    )

    spark.sql(f"""
        WITH staging_table AS (
            SELECT
                CAST(CONCAT(
                    YEAR(timestamp), '-', 
                    LPAD(MONTH(timestamp), 2, '0'), '-', 
                    LPAD(DAY(timestamp), 2, '0'), ' ',
                    LPAD(HOUR(timestamp), 2, '0'), ':',
                    LPAD(FLOOR(MINUTE(timestamp) / 15) * 15, 2, '0'), ':00'
                ) AS TIMESTAMP) AS home_power_reading_key,
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

    spark.stop()
    

if __name__ == "__main__":
    date = sys.argv[1]
    logging.basicConfig(level = "INFO")
    main(date)