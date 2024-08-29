from dag_fxn import *

default_args = {
    'owner': 'nsano',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='VA_Database',
    description='Automates the creation and insertion of data into a table',
    default_args=default_args,
    start_date=datetime(2024, 8, 18),
    schedule_interval=None
) as dag:
    task1 = PostgresOperator(
        task_id='create_postgres_table',
        postgres_conn_id='postgres_localhost',
        sql="""
            CREATE TABLE banks (
                mapID SERIAL PRIMARY KEY,       
                entityID INTEGER,               
                entityType VARCHAR(50),         
                balance DECIMAL(10, 2),        
                mapName VARCHAR(100),        
                threshold DECIMAL(10, 2), 
                usage VARCHAR(100),
                email VARCHAR(100)
);
        """
    )

    task2 = PostgresOperator(
        task_id='insert_into_table',
        postgres_conn_id='postgres_localhost',
        sql="""
            INSERT INTO banks (entityID, entityType, balance, mapName, threshold, usage, email)
            VALUES (1001, 'WALLET', 100.75, 'TIGO_COLLECTIONS', 1000, 'COLLECTIONS', 'JohnDoe@example.com'),
                (1002, 'WALLET', 500.00, 'TIGO_DISBURSEMENTS', 10000, 'DISBURSEMENTS', 'JohnDoe@example.com'),
                (1003, 'WALLET', 22000.34, 'AIRTEL_COLLECTIONS', 1, 'COLLECTIONS', 'JohnDoe@example.com'),
                (1004, 'WALLET', 600.50, 'AIRTEL_DISBURSEMENTS', 10000, 'DISBURSEMENTS', 'JohnDoe@example.com'),      
                (1005, 'WALLET', 17000.00, 'MTN_COLLECTIONS', 1, 'COLLECTIONS', 'JohnDoe@example.com'),
                (1006, 'WALLET', 72000.00, 'MTN_DISBURSEMENTS', 10000, 'DISBURSEMENTS', 'JohnDoe@example.com');
                """
    )

    
    task1 >> task2
