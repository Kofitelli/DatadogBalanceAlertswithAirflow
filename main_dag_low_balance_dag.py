from dag_fxn import *

initialize(**options)

default_args = {
    'owner': '@company',
    'start_date': datetime(2024, 8, 19),
    'retries': 1,
}

with DAG(
    dag_id='check_virtual_accounts',
    default_args=default_args,
    description='Checks balance and sends alerts',
    schedule_interval='@hourly',
    catchup=False
) as dag:
    check_balances_task = PythonOperator(
        task_id='check_balances',
        python_callable=check_balances,
        
    )

    low_balance_alerts_task = PythonOperator(
        task_id='send_low_balance_alerts',
        python_callable=low_balance_alerts,)

    trigger_daily_alert_dag = TriggerDagRunOperator(
        task_id='send_daily_alerts',
        trigger_dag_id='daily_balance_alerts',
        wait_for_completion=False
    )

    check_balances_task >> low_balance_alerts_task, trigger_daily_alert_dag
   
    
