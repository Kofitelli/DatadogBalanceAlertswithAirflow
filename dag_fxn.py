from airflow.utils.email import send_email
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datadog import initialize, statsd
from datetime import datetime
from datadog_api_client import ApiClient, Configuration
from datadog_api_client.v2.api.metrics_api import MetricsApi
from datadog_api_client.v2.model.metric_intake_type import MetricIntakeType
from datadog_api_client.v2.model.metric_payload import MetricPayload
from datadog_api_client.v2.model.metric_point import MetricPoint
from datadog_api_client.v2.model.metric_resource import MetricResource
from datadog_api_client.v2.model.metric_series import MetricSeries
import certifi

configuration = Configuration(
     host='https://us5.datadoghq.com',
     ssl_ca_cert=certifi.where()
)
configuration.api_key['apiKeyAuth']='API_KEY'
configuration.api_key['appKeyAuth']='APP_KEY'



def send_daily_alerts(**kwargs):
    hook = PostgresHook(postgres_conn_id="postgres_localhost")
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    cursor.execute("SELECT email, mapname, balance, threshold FROM banks")
    results = cursor.fetchall()
    for result in results:
        email = result[0]
        mapname = result[1]
        balance = result[2]
        threshold = result[3]
        
        status = 'below_threshold' if balance < threshold else 'above_threshold'
        
        push_to_datadog(mapname, balance, status)
        
        send_alert(
            email,
            f'Daily Alert: Virtual Account Balance for {mapname}',
            f'<p>Dear {mapname},</p><p>Your virtual account balance is ${balance}.</p>'
        )

def check_balances(**kwargs):
    hook = PostgresHook(postgres_conn_id="postgres_localhost")
    conn = hook.get_conn()
    cursor = conn.cursor()
    below_threshold = []
    below_threshold_email = []
    below_threshold_balance = []
    below_threshold_threshold = []
    
    cursor.execute("SELECT mapname, email, balance, threshold FROM banks WHERE balance < threshold")
    column_names = [desc[0] for desc in cursor.description]
    rows = [dict(zip(column_names, row)) for row in cursor.fetchall()]
    
    for row in rows:
        mapname = row['mapname']
        email = row['email']
        balance = float(row['balance'])
        threshold = float(row['threshold'])
        
        status = 'below_threshold' if balance < threshold else 'above_threshold'
        print(f'balance from list {balance}')
        push_to_datadog(mapname, balance, status)

        below_threshold.append(mapname)
        below_threshold_email.append(email)
        below_threshold_balance.append(balance)
        below_threshold_threshold.append(threshold)
    kwargs['ti'].xcom_push(key='below_threshold', value=below_threshold)
    kwargs['ti'].xcom_push(key='below_threshold_email', value=below_threshold_email)
    kwargs['ti'].xcom_push(key='below_threshold_balance', value=below_threshold_balance)
    kwargs['ti'].xcom_push(key='below_threshold_threshold', value=below_threshold_threshold)
    cursor.close()
    conn.close()
    print(below_threshold_email, below_threshold_balance, below_threshold_threshold, below_threshold)

def low_balance_alerts(**kwargs):
    ti = kwargs['ti']
    below_threshold = ti.xcom_pull(key='below_threshold', task_ids='check_balances')
    below_threshold_email = ti.xcom_pull(key='below_threshold_email', task_ids='check_balances')
    below_threshold_balance = ti.xcom_pull(key='below_threshold_balance', task_ids='check_balances')
    below_threshold_threshold = ti.xcom_pull(key='below_threshold_threshold', task_ids='check_balances')

    for email, mapname, balance, threshold in zip(below_threshold_email, below_threshold, below_threshold_balance, below_threshold_threshold):
        print(below_threshold_email)
        send_alert(
            email,
            f'Critical Alert: Virtual Account Balance Low for {mapname}',
            f'<p>Dear {mapname},</p><p>Your virtual account balance has fallen below the threshold.'
            f'Your balance currently sits at ${balance}, while the threshold is ${threshold}.</p>Please take immediate action to replenish the funds.</p>'
        )
   
def send_alert(email, subject, message, **kwargs):
    send_email(email, subject, message, **kwargs)

def push_to_datadog(mapname, balance, status, tags=None):
    balance = float(balance)
    metric_name = f'virtual_accounts.balance.{mapname}'
    status_tag = f'status:{status}'
    
    body = MetricPayload(
        series=[
            MetricSeries(
                metric=metric_name,
                type=MetricIntakeType.GAUGE,
                points=[
                    MetricPoint(
                        timestamp=int(datetime.now().timestamp()),
                        value=balance,
                    ),
                ],
                resources=[
                    MetricResource(
                        name="localhost",
                        type="host",
                    ),
                ],
                tags=tags or ["custom:metric_API", "version:latest", status_tag],
            ),
        ],
    )

    with ApiClient(configuration) as api_client:
        api_instance = MetricsApi(api_client)
        response = api_instance.submit_metrics(body=body)
        print(response)

