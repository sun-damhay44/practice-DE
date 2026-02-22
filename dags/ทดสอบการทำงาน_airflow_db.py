from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime

with DAG(
    dag_id='0_setup_project_database',
    start_date=datetime(2025, 1, 1),
    schedule='@once',  # แก้จาก schedule_interval เป็น schedule
    catchup=False
) as dag:

    create_table = SQLExecuteQueryOperator(
        task_id='create_table_test',
        conn_id='my_dw_conn',
        sql="""
            CREATE TABLE IF NOT EXISTS project_logs (
                id SERIAL PRIMARY KEY,
                log_message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            INSERT INTO project_logs (log_message) VALUES ('System is Ready for Airflow 3.1.6 Project!');
        """
    )