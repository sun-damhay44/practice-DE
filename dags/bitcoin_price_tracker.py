from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta
import requests

# 2. เพิ่ม default_args ตรงนี้
default_args = {
    'owner': 'airflow',
    'retries': 3,                               # ถ้าพัง (เช่น API ล่ม) ให้ลองใหม่ 3 ครั้ง
    'retry_delay': timedelta(minutes=1),        # รอ 1 นาทีก่อนลองใหม่ (เพราะรันทุกนาทีอยู่แล้ว)
    'retry_exponential_backoff': True,         # รอบถัดไปให้รอนานขึ้นนิดนึง
    # --- ส่วนที่เพิ่มสำหรับ Email ---
    'email': ['charnnarong.251044@gmail.com'], # ใส่ Email ของคุณ (ใส่หลายคนได้)
    'email_on_failure': True,             # ส่งเมลเมื่อพังสนิท
    'email_on_retry': False,             # ไม่ต้องส่งทุกครั้งที่ retry (เดี๋ยวเมลเต็ม)
}

# ฟังก์ชันดึงราคาจาก API
def fetch_btc_price():
    url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd"
    response = requests.get(url)
    data = response.json()
    return data['bitcoin']['usd']

with DAG(
    dag_id='1_bitcoin_price_tracker',
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule='* * * * *', # รันทุกนาที
    catchup=False
) as dag:

    # 1. สร้างตารางเก็บราคา (ถ้ายังไม่มี)
    create_table = SQLExecuteQueryOperator(
        task_id='create_btc_table',
        conn_id='my_dw_conn',
        sql="""
            CREATE TABLE IF NOT EXISTS btc_prices (
                id SERIAL PRIMARY KEY,
                price_usd FLOAT,
                fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """
    )

    # 2. ดึงข้อมูลผ่าน Python
    get_price = PythonOperator(
        task_id='fetch_current_price',
        python_callable=fetch_btc_price
    )

    # 3. บันทึกลง Database
    # ใช้ XCom เพื่อดึงค่าจาก task 'get_price' มาใส่ใน SQL
    save_price = SQLExecuteQueryOperator(
        task_id='save_price_to_db',
        conn_id='my_dw_conn',
        sql="INSERT INTO btc_prices (price_usd) VALUES ({{ task_instance.xcom_pull(task_ids='fetch_current_price') }});"
    )

    create_table >> get_price >> save_price