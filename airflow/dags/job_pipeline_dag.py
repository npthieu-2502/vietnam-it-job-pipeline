from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'it_job_market_pipeline',
    default_args=default_args,
    description='Pipeline thu thập và chuẩn hoá dữ liệu Job Market',
    schedule_interval=timedelta(days=1), # Chạy định kỳ mỗi ngày
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['de_project'],
) as dag:

    # Task 1: Chạy code Python cào dữ liệu (Extract & Load)
    # Vì chạy trong Docker, cần cấp biến POSTGRES_HOST
    scrape_task = BashOperator(
        task_id='export_jobs_to_db',
        bash_command='python /opt/airflow/src/scraper.py',
        env={"POSTGRES_HOST": "postgres"} 
    )

    # Task 2: Chạy dbt để Transform dữ liệu
    # Thêm PATH để Airflow tìm thấy file thực thi của dbt
    dbt_run_task = BashOperator(
        task_id='dbt_transform_skills',
        bash_command='export PATH=$PATH:/home/airflow/.local/bin && cd /opt/airflow/dbt_transform && dbt run --profiles-dir .',
        env={"POSTGRES_HOST": "postgres"}
    )

    # Khai báo luồng chạy: Scrape xong mới được chạy dbt
    scrape_task >> dbt_run_task
