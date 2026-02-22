import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# เพิ่ม Path เพื่อให้ Airflow หาไฟล์ใน scripts เจอ
sys.path.append('/opt/airflow/scripts/bitcoin_price_tracker_version3')
from extract_api import fetch_bitcoin_to_json, load_parquet_to_postgres

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='bitcoin_pyspark_pipeline_v3',
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule='@hourly',
    catchup=False
) as dag:

    # Task 1: ดึงข้อมูล (เรียกใช้ Python Function จากไฟล์ extract_api.py)
    extract_task = PythonOperator(
        task_id='extract_from_api',
        python_callable=fetch_bitcoin_to_json
    )

    # Task 2: ประมวลผลด้วย Spark (เรียกใช้ไฟล์ transform_spark.py)
    # ในเครื่องคุณอาจใช้ BashOperator สั่ง spark-submit 
    # แต่ถ้าเป็นงานจริงจะใช้ DatabricksOperator ครับ
    transform_task = BashOperator(
        task_id='spark_transformation',
        bash_command='python3 /opt/airflow/scripts/bitcoin_price_tracker_version3/transform_spark.py'
    )

    # Task 3: โหลดข้อมูลที่ Transform แล้วเข้า Database
    load_task = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_parquet_to_postgres
    )

    extract_task >> transform_task >> load_task