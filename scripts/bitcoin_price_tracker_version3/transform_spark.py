from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

# 1. สร้าง Spark Session พร้อมตั้งค่าให้รันใน Docker ได้ (ใช้ทรัพยากรน้อย)
spark = SparkSession.builder \
    .appName("BitcoinTransform") \
    .master("local[1]") \
    .config("spark.driver.memory", "512m") \
    .getOrCreate()

# 2. อ่านข้อมูลดิบ (JSON)
# ตรวจสอบว่า Path ตรงกับที่ตั้งไว้ใน extract_api.py
RAW_PATH = "/tmp/bitcoin_project/raw/"

if not os.path.exists("/tmp/bitcoin_project/raw"):
    print("No raw data folder found.")
    spark.stop()
    exit(0)

df = spark.read.json(RAW_PATH)

# 3. Transformation: แปลงจาก Nested เป็น Flat
df_flat = df.selectExpr(
    "bitcoin.usd as bitcoin", 
    "ethereum.usd as ethereum", 
    "dogecoin.usd as dogecoin"
)

# 4. คำนวณและ Clean ข้อมูล: แปลงจาก Wide เป็น Long Format
# เพิ่ม .cast("double") ให้กับทุกตัวแปรราคาเพื่อให้ Type ตรงกัน
result_df = df_flat.select(
    F.expr("""
        stack(3, 
            'bitcoin', cast(bitcoin as double), 
            'ethereum', cast(ethereum as double), 
            'dogecoin', cast(dogecoin as double)
        ) as (coin_name, price_usd)
    """)
).withColumn("fetched_at", F.current_timestamp())

# 5. การบันทึกข้อมูล (เปลี่ยนจาก Parquet เป็นการเตรียมส่งเข้า Database)
# ในงานจริงเรามักจะเซฟเป็น Parquet ลง Data Lake ก่อน 
OUTPUT_PATH = "/tmp/bitcoin_project/gold/prices"
result_df.write.mode("append").parquet(OUTPUT_PATH)

# แสดงผลลัพธ์เพื่อตรวจสอบใน Log ของ Airflow
result_df.show()

print(f"Transformation complete. Data saved to {OUTPUT_PATH}")

spark.stop()