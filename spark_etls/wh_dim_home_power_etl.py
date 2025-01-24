from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import logging


def main():
    spark = (
        SparkSession
        .builder
        .appName("create raw home power readings")
        .getOrCreate()
    )

    spark.sql("""
        MERGE INTO SolarX_WH.dim_home dim_home
        USING (
            SELECT 
                home_key, 
                SUM(min_consumption_power_wh) AS min_consumption_power_wh,
                SUM(max_consumption_power_wh) AS max_consumption_power_wh 
            FROM SolarX_WH.dim_home_appliances
            GROUP BY home_key
        ) dim_app
        ON dim_home.home_id = dim_app.home_key AND dim_home.current_flag = TRUE

        WHEN MATCHED AND (
            dim_home.max_consumption_power_wh != dim_app.max_consumption_power_wh OR
            dim_home.min_consumption_power_wh != dim_app.min_consumption_power_wh
        ) THEN UPDATE SET 
            dim_home.end_date = NOW(),
            dim_home.current_flag = FALSE;
    """)

    spark.sql("""
        MERGE INTO SolarX_WH.dim_home dim_home
        USING (
            SELECT 
                home_key, 
                SUM(min_consumption_power_wh) AS min_consumption_power_wh,
                SUM(max_consumption_power_wh) AS max_consumption_power_wh 
            FROM SolarX_WH.dim_home_appliances
            GROUP BY home_key
        ) dim_app
        ON dim_home.home_id = dim_app.home_key AND dim_home.current_flag = TRUE

        WHEN NOT MATCHED THEN 
        INSERT (
            home_key,
            home_id,
            min_consumption_power_wh, 
            max_consumption_power_wh,
            start_date,
            end_date,
            current_flag
        ) VALUES (
            (SELECT COUNT(*) FROM SolarX_WH.dim_home) + 1,
            1,
            dim_app.min_consumption_power_wh,
            dim_app.max_consumption_power_wh,
            NOW(),
            NULL,
            TRUE
        );
    """)

    logging.info(f"""wh_dim_home_power_etl -> Load into dim_home iceberg table successfully""")

    spark.stop()
    

if __name__ == "__main__":
    logging.basicConfig(level = "INFO")
    main()