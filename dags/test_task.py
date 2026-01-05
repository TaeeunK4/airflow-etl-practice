from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import snowflake.connector

# 기존에 쓰시던 설정값들
def test_snowflake_connection():
    conn = snowflake.connector.connect(
        user='AIRFLOW_WORKER', # image_5ca83c.png의 Login name
        password='Loudiere2304!@#',
        account='MLMIQGV-DQ71425', # 하이픈 사용
        warehouse='COMPUTE_WH',
        database='IVE_DATA',
        schema='RAW_DATA'
    )
    cur = conn.cursor()
    cur.execute("SELECT CURRENT_VERSION()")
    print(f"Connected! version: {cur.fetchone()[0]}")
    cur.close()
    conn.close()

with DAG(
    dag_id="test_direct_connection",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    test_task = PythonOperator(
        task_id="test_conn_task",
        python_callable=test_snowflake_connection
    )