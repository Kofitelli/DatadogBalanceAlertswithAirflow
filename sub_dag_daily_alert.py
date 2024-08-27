from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from dag_fxn import *

options = {
     'statsd_host':'127.0.0.1',
    'statsd_port':8125
}

initialize(**options)

default_args = {
    'owner': 'nsano_Ltd',
    'start_date': datetime(2024, 8, 19),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='daily_balance_alerts',
    default_args=default_args,
    description='Sends daily balance alerts to all accounts',
    schedule_interval='@daily', 
    catchup=False
) as dag:
    send_daily_alerts_task = PythonOperator(
        task_id='send_daily_alerts',
        python_callable=send_daily_alerts,
    )

    send_daily_alerts_task

