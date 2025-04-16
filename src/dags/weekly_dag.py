from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

SQL_PATH = 'migrations'

postgres_conn_id = 'postgresql_de'

args = {
    "owner": "student",
    'email': ['student@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

business_dt = '{{ ds }}'

with DAG(
        'weekly_update_f_customer_retention',
        default_args=args,
        description='Weekly update f_customer_retention',
        catchup=False,
        start_date=datetime.today() - timedelta(days=7),
        schedule_interval='0 0 * * 1'
) as dag:
 
    update_f_customer_retention = PostgresOperator(
        task_id='update_f_customer_retention',
        postgres_conn_id=postgres_conn_id,
        sql=f"{SQL_PATH}/mart.f_customer_retention.sql",
        parameters={
            'date': {business_dt}  # Конечная дата - сегодня
        }
    )    

    update_f_customer_retention    
