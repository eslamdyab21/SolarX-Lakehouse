from pyspark.sql import SparkSession
import logging


def load_2_iceberg(spark):
    

    spark.sql("""
        MERGE INTO SolarX_WH.dim_solar_panel dim_solar_panel
        USING SolarX_Raw_Transactions.solar_panel solar_panel_raw
        ON dim_solar_panel.solar_panel_id = solar_panel_raw.id AND dim_solar_panel.current_flag = TRUE

        WHEN MATCHED AND (
            dim_solar_panel.capacity_kwh != solar_panel_raw.capacity_kwh OR
            dim_solar_panel.intensity_power_rating_wh != solar_panel_raw.intensity_power_rating OR
            dim_solar_panel.temperature_power_rating_c != solar_panel_raw.temperature_power_rating
        ) THEN UPDATE SET
            dim_solar_panel.end_date   = NOW(),
            dim_solar_panel.current_flag = FALSE;
    """)


    spark.sql("""
        MERGE INTO SolarX_WH.dim_solar_panel dim_solar_panel
        USING SolarX_Raw_Transactions.solar_panel solar_panel_raw
        ON dim_solar_panel.solar_panel_id = solar_panel_raw.id AND dim_solar_panel.current_flag = TRUE

        WHEN NOT MATCHED THEN 
        INSERT (
            solar_panel_key,
            solar_panel_id,
            name, 
            capacity_kwh,
            intensity_power_rating_wh,
            temperature_power_rating_c,
            start_date,
            end_date,
            current_flag
        ) VALUES (
            CAST(CONCAT(solar_panel_raw.id, date_format(NOW(), 'yyyyMMdd')) AS INT),
            solar_panel_raw.id,
            solar_panel_raw.name,
            solar_panel_raw.capacity_kwh,
            solar_panel_raw.intensity_power_rating,
            solar_panel_raw.temperature_power_rating,
            NOW(),
            NULL,
            TRUE
        );
    """)

    logging.info(f"""wh_dim_solar_panel_power_etl -> Load into dim_solar_panel iceberg table successfully""")


    

if __name__ == "__main__":
    logging.basicConfig(level = "INFO")

    spark = (
        SparkSession
        .builder
        .appName("create raw solar panel power readings")
        .getOrCreate()
    )

    load_2_iceberg(spark)


    spark.stop()