from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import logging




def main():
    spark = (
        SparkSession
        .builder
        .appName("create raw solar panel power")
        .getOrCreate()
    )


    spark.sql("""
        INSERT INTO SolarX_Raw_Transactions.solar_panel (id, name, capacity_kwh, intensity_power_rating, temperature_power_rating) VALUES
            (1, 'roof panel', 3, 1000, 25),
            (2, 'pole panel', 5, 1300, 25),
            (3, 'flush panel', 10, 1500, 25);
    """)

    logging.info(f"""raw-solar-panel-power-etl -> Load solar_panel successfully into iceberg""")



if __name__ == "__main__":
    logging.basicConfig(level = "INFO")
    main()