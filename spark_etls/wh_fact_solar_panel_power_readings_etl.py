from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
import logging
import sys


staging_query = """
    SELECT
        panel_id,
        TIMESTAMP(FLOOR(UNIX_MICROS(timestamp) / (15 * 60 * 1000000)) * (15 * 60)) AS truncated_timestamp,
        SUM(generation_power_wh) AS generation_power_wh
    FROM 
        SolarX_Raw_Transactions.solar_panel_readings
    WHERE 
        DATE(timestamp) = DATE('{date}')
    GROUP BY 
        panel_id, truncated_timestamp
"""

dim_solar_panel_current_query = """
    SELECT 
        solar_panel_key,	
        solar_panel_id
    FROM 
        SolarX_WH.dim_solar_panel
    WHERE 
        dim_solar_panel.current_flag = TRUE
"""

def broadcast_join(spark, date):
    staging_df = spark.sql(staging_query.replace("{date}", date))
    dimension_df = spark.sql(dim_solar_panel_current_query)

    # Broadcast the smaller dimension table for the join
    joined_df = staging_df.join(
        broadcast(dimension_df),
        (staging_df.panel_id == dimension_df.solar_panel_id),
        "left"
    )


    logging.info(f"""wh_fact_solar_panel_power_readings_etl -> Broadcast join successfully""")
    return joined_df



def load_2_iceberg(joined_df):

    joined_df.createOrReplaceTempView("staging_temp_view")

    spark.sql(f"""
        MERGE INTO SolarX_WH.fact_solar_panel_power_readings AS target
        USING staging_temp_view AS source
        ON target.solar_panel_id = source.panel_id AND target.date_key = source.truncated_timestamp
            
        WHEN NOT MATCHED THEN
            INSERT (solar_panel_key, 
                    date_key, 
                    solar_panel_id,
                    generation_power_wh
            
            ) 
            VALUES (source.solar_panel_key, 
                    source.truncated_timestamp,
                    source.panel_id,
                    source.generation_power_wh     
            );
    """)

    logging.info(f"""wh_fact_solar_panel_power_readings_etl -> Load into fact_solar_panel_power_readings iceberg table successfully""")

    

if __name__ == "__main__":
    logging.basicConfig(level = "INFO")
    date = sys.argv[1]

    spark = (
        SparkSession
        .builder
        .appName("load wh fact solar panel power readings")
        .getOrCreate()
    )

    joined_df = broadcast_join(spark, date)
    load_2_iceberg(joined_df)

    spark.stop()
