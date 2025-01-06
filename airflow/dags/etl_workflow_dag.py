from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}


with DAG(
    'etl_workflow',
    default_args=default_args,
    # schedule_interval='@daily',
    catchup=False,
) as dag:
    
    create_raw_schema = SSHOperator(
        task_id='create_raw_schema',
        command='/opt/spark/bin/spark-submit --master spark://spark-master:7077 --num-executors 6 --executor-cores 1 --executor-memory 512M /home/iceberg/etl_scripts/create_raw_schema.py',
        ssh_conn_id='spark_master_ssh',
        dag=dag,
    )

    check_schema_exists = SSHOperator(
        task_id='check_schema_exists',
        command='/opt/spark/bin/spark-submit --master spark://spark-master:7077 --num-executors 6 --executor-cores 1 --executor-memory 512M /home/iceberg/etl_scripts/check_schema_exists.py',
        ssh_conn_id='spark_master_ssh',
        dag=dag,
    )
    

    create_raw_schema >> check_schema_exists
