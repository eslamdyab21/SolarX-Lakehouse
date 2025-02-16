from pyspark.sql import SparkSession
import logging


def load_2_iceberg(spark):
    

    spark.sql("""
        WITH battery_info AS (
            SELECT 
                timestamp,
                CAST(SPLIT_PART(battery_name, '_', 2) AS INT) as battery_id,
                battery.battery_name,
                battery.capacity_kwh,
                battery.max_charge_speed_w,
                battery.max_output_w
            FROM SolarX_Raw_Transactions.battery_readings battery
            ORDER BY timestamp desc
            LIMIT 3    
        )


        MERGE INTO SolarX_WH.dim_battery dim_battery
        USING battery_info battery_raw
        ON dim_battery.battery_id = battery_raw.battery_id AND dim_battery.current_flag = TRUE

        WHEN MATCHED AND (
            dim_battery.capacity_kwh != battery_raw.capacity_kwh OR
            dim_battery.max_charge_speed_w != battery_raw.max_charge_speed_w OR
            dim_battery.max_output_w != battery_raw.max_output_w
        ) THEN UPDATE SET
            dim_battery.end_date   = NOW(),
            dim_battery.current_flag = FALSE;
    """)


    spark.sql("""
        WITH battery_info AS (
            SELECT 
                timestamp,
                CAST(SPLIT_PART(battery_name, '_', 2) AS INT) as battery_id,
                battery.battery_name,
                battery.capacity_kwh,
                battery.max_charge_speed_w,
                battery.max_output_w
            FROM SolarX_Raw_Transactions.battery_readings battery
            ORDER BY timestamp desc
            LIMIT 3    
        )


        MERGE INTO SolarX_WH.dim_battery dim_battery
        USING battery_info battery_raw
        ON dim_battery.battery_id = battery_raw.battery_id AND dim_battery.current_flag = TRUE

        WHEN NOT MATCHED THEN 
        INSERT (
            battery_key,
            battery_id,
            name, 
            capacity_kwh,
            max_charge_speed_w,
            max_output_w,
            start_date,
            end_date,
            current_flag
        ) VALUES (
            CAST(CONCAT(battery_raw.battery_id, date_format(timestamp, 'yyyyMMdd')) AS INT),
            battery_raw.battery_id,
            battery_raw.battery_name,
            battery_raw.capacity_kwh,
            battery_raw.max_charge_speed_w,
            battery_raw.max_output_w,
            NOW(),
            NULL,
            TRUE
        );
    """)

    logging.info(f"""wh_dim_battery_power_etl -> Load into dim_battery iceberg table successfully""")


    

if __name__ == "__main__":
    logging.basicConfig(level = "INFO")

    spark = (
        SparkSession
        .builder
        .appName("create raw battery power readings")
        .getOrCreate()
    )

    load_2_iceberg(spark)


    spark.stop()