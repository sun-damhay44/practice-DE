import requests
import json
import os
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine

COINS = ['bitcoin', 'ethereum', 'dogecoin']
RAW_DATA_PATH = "/tmp/bitcoin_project/raw" # ในงานจริงอาจเป็น S3 หรือ Data Lake

def fetch_bitcoin_to_json():
    ids = ",".join(COINS)
    url = f"https://api.coingecko.com/api/v3/simple/price?ids={ids}&vs_currencies=usd"
    
    response = requests.get(url)
    data = response.json()
    
    # สร้าง folder ถ้ายังไม่มี
    os.makedirs(RAW_DATA_PATH, exist_ok=True)
    
    # บันทึกเป็นไฟล์ JSON แยกตามเวลาที่ดึง
    filename = f"crypto_raw_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(f"{RAW_DATA_PATH}/{filename}", 'w') as f:
        json.dump(data, f)
    
    print(f"Successfully saved raw data to {filename}")

if __name__ == "__main__":
    fetch_bitcoin_to_json()

def load_parquet_to_postgres():
    # 1. ระบุ Path ของไฟล์ Parquet ที่ Spark สร้างไว้
    gold_path = "/tmp/bitcoin_project/gold/prices"
    
    if not os.path.exists(gold_path):
        print("ไม่พบไฟล์ Parquet สำหรับการโหลด")
        return

    # 2. อ่านไฟล์ Parquet เข้ามาเป็น Pandas DataFrame
    df = pd.read_parquet(gold_path)
    
    # 3. ตั้งค่าการเชื่อมต่อ Postgres (ใช้ชื่อ Service ใน Docker)
    # format: postgresql://username:password@hostname:port/database
    engine = create_engine('postgresql://sun:sun251044@data-warehouse:5432/target_db')
    
    # 4. เขียนข้อมูลลง Table ใน Postgres
    # if_exists='append' คือการเพิ่มข้อมูลต่อท้ายไปเรื่อยๆ
    df.to_sql('bitcoin_prices', engine, if_exists='append', index=False)
    
    print("โหลดข้อมูลเข้า Postgres สำเร็จแล้ว! ตรวจสอบใน pgAdmin ได้เลย")