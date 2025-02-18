from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from datetime import datetime
import base64


def decode_xcom_output(encoded_output):
    decoded_bytes = base64.b64decode(encoded_output)
    decoded_str = decoded_bytes.decode('utf-8')
    return decoded_str


      
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


with DAG('etl_kafka_workflow', default_args=default_args, catchup=False) as dag:
    source_data_type = Variable.get("source_data_type")
    date = Variable.get("date")

    # ------------- schema ------------------
    check_schema_exists = SSHOperator(
        task_id='check_schema_exists',
        command='/bin/bash /home/iceberg/etl_scripts/airflow_bash_wrapper.sh check_schema_exists.py',
        ssh_conn_id='spark_master_ssh',
        dag=dag,
        do_xcom_push=True,
    )


    check_wh_schema_exists_output = BranchPythonOperator(
        task_id='check_wh_schema_exists_output',
        python_callable=process_output_from_check_wh_schema_exists,
        do_xcom_push=True,
        provide_context=True
    )


    create_wh_schema = SSHOperator(
        task_id='create_wh_schema',
        command='/bin/bash /home/iceberg/etl_scripts/airflow_bash_wrapper.sh create_wh_schema.py',
        ssh_conn_id='spark_master_ssh',
        dag=dag,
    )



    merge_task = DummyOperator(
        task_id='merge_task',
        trigger_rule='none_failed_or_skipped',  # Continue regardless of which branch was taken
    )

    skip_create_wh_schema = DummyOperator(
        task_id='skip_create_wh_schema',
    )




    # ------------- wh etl ------------------
    # ----- Home etl ------
    wh_dim_home_appliances_power_etl = SSHOperator(
        task_id='wh_dim_home_appliances_power_etl',
        command=f"""/bin/bash /home/iceberg/etl_scripts/airflow_bash_wrapper.sh wh_dim_home_appliances_power_etl.py""",
        ssh_conn_id='spark_master_ssh',
        dag=dag,
    )


    wh_dim_home_power_etl = SSHOperator(
        task_id='wh_dim_home_power_etl',
        command=f"""/bin/bash /home/iceberg/etl_scripts/airflow_bash_wrapper.sh wh_dim_home_power_etl.py""",
        ssh_conn_id='spark_master_ssh',
        dag=dag,
    )


    wh_fact_home_power_readings_etl = SSHOperator(
        task_id='wh_fact_home_power_readings_etl',
        command=f"""/bin/bash /home/iceberg/etl_scripts/airflow_bash_wrapper.sh wh_fact_home_power_readings_etl.py {source_data_type} {date}""",
        ssh_conn_id='spark_master_ssh',
        dag=dag,
    )



    # ----- Solar panel etl ------
    wh_dim_solar_panel_power_etl = SSHOperator(
        task_id='wh_dim_solar_panel_power_etl',
        command=f"""/bin/bash /home/iceberg/etl_scripts/airflow_bash_wrapper.sh wh_dim_solar_panel_power_etl.py""",
        ssh_conn_id='spark_master_ssh',
        dag=dag,
    )


    wh_fact_solar_panel_power_readings_etl = SSHOperator(
        task_id='wh_fact_solar_panel_power_readings_etl',
        command=f"""/bin/bash /home/iceberg/etl_scripts/airflow_bash_wrapper.sh wh_fact_solar_panel_power_readings_etl.py {date}""",
        ssh_conn_id='spark_master_ssh',
        dag=dag,
    )






    check_schema_exists >> check_wh_schema_exists_output
    check_wh_schema_exists_output >> [skip_create_wh_schema, create_wh_schema] >> merge_task
    merge_task >> wh_dim_home_appliances_power_etl >> wh_dim_home_power_etl >> wh_fact_home_power_readings_etl
    wh_dim_solar_panel_power_etl >> wh_fact_solar_panel_power_readings_etl