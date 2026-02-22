# ใช้ Image พื้นฐานของ Airflow เวอร์ชันที่คุณใช้อยู่
FROM apache/airflow:3.1.6

# เปลี่ยนเป็น User Root เพื่อติดตั้ง Library ระบบ
USER root

# ติดตั้ง Java (จำเป็นสำหรับ Spark)
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-17-jre-headless \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# เปลี่ยนกลับเป็น User Airflow เพื่อลง Python Library
USER airflow

# ติดตั้ง Library ที่ต้องใช้
RUN pip install --no-cache-dir requests pyspark