from pyspark.sql import SparkSession
import logging



def main():
    spark = (
        SparkSession
        .builder
        .appName("create raw solar panel power")
        .getOrCreate()
    )

    source_df = spark.createDataFrame([
    (1, 'roof panel', 3, 1000, 25),
    (2, 'pole panel', 5, 1300, 25),
    (3, 'flush panel', 10, 1500, 25)
    ], ["id", "name", "capacity_kwh", "intensity_power_rating", "temperature_power_rating"])
    
    source_df.createOrReplaceTempView("source_view")

    spark.sql("""
        MERGE INTO SolarX_Raw_Transactions.solar_panel target
        USING source_view source
        ON target.id = source.id
        WHEN MATCHED THEN UPDATE SET
            target.capacity_kwh = source.capacity_kwh,
            target.intensity_power_rating = source.intensity_power_rating,
            target.temperature_power_rating = source.temperature_power_rating,
        WHEN NOT MATCHED THEN 
            INSERT (id, name, capacity_kwh, intensity_power_rating, temperature_power_rating)
            VALUES (source.id, source.name, source.capacity_kwh, source.intensity_power_rating, source.temperature_power_rating)
    """)

    logging.info(f"""raw-solar-panel-power-etl -> Load solar_panel successfully into iceberg""")


    spark.stop()

    
if __name__ == "__main__":
    logging.basicConfig(level = "INFO")
    main()