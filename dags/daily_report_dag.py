from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import pandas as pd

default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def export_orders():
    try:
        conn = psycopg2.connect(
            host="host.docker.internal",
            dbname="orders_db",
            user="postgres",
            password="root"
        )
        df = pd.read_sql("SELECT * FROM orders ORDER BY created_at DESC", conn)
        output_path = "/opt/airflow/reports/daily_orders.csv"
        df.to_csv(output_path, index=False)
        conn.close()
        print(f"✅ Export réalisé: {len(df)} commandes")
        return f"Success: {len(df)} orders exported"
    except Exception as e:
        print(f"❌ Erreur export: {e}")
        raise

with DAG(
    dag_id="daily_report",
    default_args=default_args,
    description='Export daily orders to CSV',
    schedule_interval="@daily",
    catchup=False,
    tags=['reporting', 'orders']
) as dag:
    
    export_task = PythonOperator(
        task_id="export_orders",
        python_callable=export_orders
    )
