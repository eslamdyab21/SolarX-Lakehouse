from pyspark.sql import SparkSession
import logging


def namespace_exists(spark, desired_namespace):
    namespaces = spark.catalog.listDatabases()
    for namespace in namespaces:
        if desired_namespace in namespace:
            return True
    return False


def main():
    spark = (
        SparkSession
        .builder
        .appName("create raw schema")
        .getOrCreate()
    )
    logging.info("check-raw-schema-exists -> successfully setup spark session")


    # -------------- SolarX_Raw_Transactions namespace-------------
    namespace = "SolarX_Raw_Transactions"
    if namespace_exists(spark, namespace):
        logging.info(f"""check-raw-schema-exists -> {namespace} exists""")
    else:
        logging.info(f"""check-raw-schema-exists -> {namespace} doesn't exists""")


    # -------------- SolarX_WH -------------
    namespace = "SolarX_WH"
    if namespace_exists(spark, namespace):
        logging.info(f"""check-wh-schema-exists -> {namespace} exists""")
    else:
        logging.info(f"""check-wh-schema-exists -> {namespace} doesn't exists""")



if __name__ == "__main__":
    logging.basicConfig(level = "INFO")
    main()