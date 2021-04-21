from airflow.models import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.contrib.hooks.ssh_hook import SSHHook
from datetime import timedelta, date
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'equity_market_pipeline',
    default_args=default_args,
    description='Spring Capital Data Pipeline',
    schedule_interval='0 18 * * 1-5',
    start_date=days_ago(1)
)

hook = SSHHook(ssh_conn_id='ssh_default')

task_ingestion = SSHOperator(
    task_id='ingestion',
    command="python3 ingestion.py",
    ssh_hook=hook,
    dag=dag
)

task_load = SSHOperator(
    task_id='load',
    command="python3 load.py",
    ssh_hook=hook,
    dag=dag
)

task_analytics = SSHOperator(
    task_id='analytics',
    command="python3 analytics.py",
    ssh_hook=hook,
    dag=dag
)

task_ingestion >> task_load >> task_analytics