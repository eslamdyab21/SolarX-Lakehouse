from pyspark.sql import SparkSession
import pandas as pd
import json
import logging


def get_home_appliances_df():
    with open('/home/iceberg/warehouse/home_appliances_consumption.json') as f:
        HOME_USAGE_POWER = json.load(f)
        HOME_USAGE_POWER.items()
        
        df = pd.DataFrame([
                {
                    "home_key" : 1,
                    "name": name,
                    "min_consumption_rating": info["consumption"][0],
                    "max_consumption_rating": info["consumption"][1],
                    "usage_time": info["time"]
                }
                for name, info in HOME_USAGE_POWER.items()
            ])
        df.index += 1 
        df.index.name = 'home_appliance_key'
    return df.reset_index()



def main():
    spark = (
        SparkSession
        .builder
        .appName("create wh home appliances power")
        .getOrCreate()
    )

    home_appliances_df = spark.createDataFrame(get_home_appliances_df())
    home_appliances_df.createOrReplaceTempView("temp_view")

    spark.sql("""
        MERGE INTO SolarX_WH.dim_home_appliances dim_app
        USING 
            (SELECT home_appliance_key        as home_appliance_key, 
                    home_key                  as home_key,
                    name                      as appliance,
                    min_consumption_rating    as min_consumption_power_wh,
                    max_consumption_rating    as max_consumption_power_wh,
                    usage_time                as usage_time
            FROM temp_view) tmp
            
        ON dim_app.home_appliance_key = tmp.home_appliance_key

        WHEN MATCHED AND (
            dim_app.min_consumption_power_wh != tmp.min_consumption_power_wh OR
            dim_app.max_consumption_power_wh != tmp.max_consumption_power_wh
        ) THEN UPDATE SET 
            dim_app.min_consumption_power_wh = tmp.min_consumption_power_wh,
            dim_app.max_consumption_power_wh = tmp.max_consumption_power_wh

        WHEN NOT MATCHED THEN INSERT *
    """)

    logging.info(f"""wh-home-power-power-etl -> Load dim_home_appliances successfully into iceberg""")


    spark.stop()

    
if __name__ == "__main__":
    logging.basicConfig(level = "INFO")
    main()