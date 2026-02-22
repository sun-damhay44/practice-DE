

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.exceptions import AirflowFailException # สำหรับสั่งให้ Task พังเมื่อ Data Quality ไม่ผ่าน
from datetime import datetime, timedelta
import requests
import logging

default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'retry_exponential_backoff': True,
    'email': ['________@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

# --- [ข. Scalability] กำหนดรายการเหรียญที่ต้องการดึง ---
COINS = ['bitcoin', 'ethereum', 'dogecoin']

# --- [ก. Data Validation] ฟังก์ชันตรวจสอบคุณภาพข้อมูล ---
def validate_data(coin, price):
    if price is None or price <= 0:
        raise AirflowFailException(f"Data Quality Error: {coin} price is invalid ({price})")
    
    # สมมติเช็ค Outlier: ถ้าราคาพุ่งเกินปกติ (เช่น Bitcoin เกิน $200,000) ให้แจ้งเตือน
    if coin == 'bitcoin' and price > 200000:
        logging.warning(f"Suspiciously high price for {coin}: {price}")
        # คุณสามารถเลือกได้ว่าจะให้พัง (Raise) หรือแค่เตือน (Log)

# --- [ค. Transformation] ฟังก์ชันดึงและแปรรูปข้อมูล ---
def fetch_and_transform_prices():
    ids = ",".join(COINS)
    url = f"https://api.coingecko.com/api/v3/simple/price?ids={ids}&vs_currencies=usd"
    
    response = requests.get(url)
    data = response.json()
    
    processed_data = []
    for coin in COINS:
        price = data.get(coin, {}).get('usd')
        
        # รัน Validation
        validate_data(coin, price)
        
        # [ค. Transformation] ตัวอย่างการแปรรูป: เพิ่มสกุลเงิน และเวลาประมวลผล
        processed_data.append({
            'coin': coin,
            'price': price,
            'timestamp': datetime.now().isoformat()
        })
        
    return processed_data

with DAG(
    dag_id='bitcoin_price_tracker_version2', # เปลี่ยนชื่อเพื่อทำเป็นโปรเจคใหม่
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule='*/5 * * * *', # เปลี่ยนเป็นทุก 5 นาทีเพื่อลดการยิง API ถี่เกินไป
    catchup=False,
    tags=['production', 'data_quality']
) as dag:

    # 1. ปรับตารางให้รองรับหลายเหรียญ
    create_table = SQLExecuteQueryOperator(
        task_id='create_crypto_table',
        conn_id='my_dw_conn',
        sql="""
            CREATE TABLE IF NOT EXISTS crypto_prices (
                id SERIAL PRIMARY KEY,
                coin_name VARCHAR(50),
                price_usd FLOAT,
                fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (coin_name, fetched_at) -- ป้องกันไม่ให้เหรียญเดิม ในเวลาเดิม บันทึกซ้ำ
            );
        """
    )

    # 2. Fetch & Transform & Validate
    get_and_process_data = PythonOperator(
        task_id='fetch_and_process_data',
        python_callable=fetch_and_transform_prices
    )

    # 3. [ข. Scalability] บันทึกข้อมูลแบบ Dynamic ด้วยการวนลูปสร้าง Task
    # หรือใช้คำสั่ง SQL เดียวที่รองรับหลายแถว
    save_data = SQLExecuteQueryOperator(
        task_id='save_all_prices_to_db',
        conn_id='my_dw_conn',
        sql="""
            {% set prices = task_instance.xcom_pull(task_ids='fetch_and_process_data') %}
            
            INSERT INTO crypto_prices (coin_name, price_usd, fetched_at) VALUES 
            {% for item in prices %}
                -- ใช้ {{ ts }} ซึ่งเป็นเวลาเริ่มรันของ Airflow เพื่อให้รันซ้ำแล้วเวลาเดิมเสมอ
                ('{{ item.coin }}', {{ item.price }}, '{{ ts }}'){{ "," if not loop.last }}
            {% endfor %}
            
            -- ส่วนที่เพิ่มขึ้นมาสำหรับ UPSERT
            ON CONFLICT (coin_name, fetched_at) 
            DO UPDATE SET 
                price_usd = EXCLUDED.price_usd;
        """
    )

    create_table >> get_and_process_data >> save_data