from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
import base64


def decode_xcom_output(encoded_output):
    decoded_bytes = base64.b64decode(encoded_output)
    decoded_str = decoded_bytes.decode('utf-8')
    return decoded_str


def process_output_from_check_raw_schema_exists(ti):
    task_output = ti.xcom_pull(task_ids='check_schema_exists')
    decoded_output = decode_xcom_output(task_output)


    if "SolarX_Raw_Transactions exists" in decoded_output:
        return "raw_home_power_readings_etl" 
    else:
        return "create_raw_schema"

      
def process_output_from_check_wh_schema_exists(ti):
    task_output = ti.xcom_pull(task_ids='check_schema_exists')
    decoded_output = decode_xcom_output(task_output)


    if "SolarX_WH exists" in decoded_output:
        return "SolarX_WH exists"
    else:
        return "create_wh_schema"
    

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
}


with DAG('etl_workflow', default_args=default_args, catchup=False) as dag:

    # ------------- schema ------------------
    check_schema_exists = SSHOperator(
        task_id='check_schema_exists',
        command='/opt/spark/bin/spark-submit --master spark://spark-master:7077 --num-executors 6 --executor-cores 1 --executor-memory 512M /home/iceberg/etl_scripts/check_schema_exists.py',
        ssh_conn_id='spark_master_ssh',
        dag=dag,
        do_xcom_push=True,
    )

    check_raw_schema_exists_output = BranchPythonOperator(
        task_id='check_raw_schema_exists_output',
        python_callable=process_output_from_check_raw_schema_exists,
        do_xcom_push=True,
        provide_context=True
    )

    # check_wh_schema_exists_output = BranchPythonOperator(
    #     task_id='check_wh_schema_exists_output',
    #     python_callable=process_output_from_check_wh_schema_exists,
    #     do_xcom_push=True,
    # )


    create_raw_schema = SSHOperator(
        task_id='create_raw_schema',
        command='/opt/spark/bin/spark-submit --master spark://spark-master:7077 --num-executors 6 --executor-cores 1 --executor-memory 512M /home/iceberg/etl_scripts/create_raw_schema.py',
        ssh_conn_id='spark_master_ssh',
        dag=dag,
    )

    # create_wh_schema = SSHOperator(
    #     task_id='create_wh_schema',
    #     command='/opt/spark/bin/spark-submit --master spark://spark-master:7077 --num-executors 6 --executor-cores 1 --executor-memory 512M /home/iceberg/etl_scripts/create_wh_schema.py',
    #     ssh_conn_id='spark_master_ssh',
    #     dag=dag,
    # )

    merge_task = DummyOperator(
        task_id='merge_task',
        trigger_rule='none_failed_or_skipped',  # Continue regardless of which branch was taken
    )




    # ------------- raw etl ------------------
    raw_home_power_readings_etl = SSHOperator(
        task_id='raw_home_power_readings_etl',
        command='/opt/spark/bin/spark-submit --master spark://spark-master:7077 --num-executors 6 --executor-cores 1 --executor-memory 512M /home/iceberg/etl_scripts/raw_home_power_readings_etl.py 2013-01-01 || true',
        ssh_conn_id='spark_master_ssh',
        dag=dag,
    )

    raw_solar_panel_power_etl = SSHOperator(
        task_id='raw_solar_panel_power_etl',
        command='/opt/spark/bin/spark-submit --master spark://spark-master:7077 --num-executors 6 --executor-cores 1 --executor-memory 512M /home/iceberg/etl_scripts/raw_solar_panel_power_etl.py || true',
        ssh_conn_id='spark_master_ssh',
        dag=dag,
    )

    raw_solar_panel_power_readings_etl = SSHOperator(
        task_id='raw_solar_panel_power_readings_etl',
        command='/opt/spark/bin/spark-submit --master spark://spark-master:7077 --num-executors 6 --executor-cores 1 --executor-memory 512M /home/iceberg/etl_scripts/raw_solar_panel_power_readings_etl.py 2013-01-01 || true',
        ssh_conn_id='spark_master_ssh',
        dag=dag,
    )



    

    check_schema_exists >> check_raw_schema_exists_output
    check_raw_schema_exists_output >> [raw_home_power_readings_etl, create_raw_schema] >> merge_task
    merge_task >> raw_solar_panel_power_etl >> raw_solar_panel_power_readings_etl
