source /home/iceberg/etl_scripts/.env

spark_python_script=$1
date=$2

/opt/spark/bin/spark-submit --master spark://spark-master:7077 --num-executors 6 --executor-cores 1 --executor-memory 512M /home/iceberg/etl_scripts/$spark_python_script $date