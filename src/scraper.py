import requests
import cloudscraper
from bs4 import BeautifulSoup
import pandas as pd
import datetime
import os
import time
import hashlib
import boto3
import io

def fetch_it_jobs():
    """
    Script Web Scraping lấy dữ liệu việc làm IT.
    Nâng cấp Phase 2: Thêm Unique Hash, Lưu vào MinIO Data Lake, và Incremental UPSERT.
    """
    print("🚀 Bắt đầu kích hoạt cỗ máy quét toàn bộ 42 trang...")
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
    }
    
    extracted_jobs = []

    scraper = cloudscraper.create_scraper()
    for page in range(1, 43):
        url = f"https://itviec.com/it-jobs?page={page}"
        print(f"▶️ Đang thâm nhập và quét dữ liệu Trang {page} / 42...")
        
        try:
            response = scraper.get(url, headers=headers)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"❌ Lỗi kết nối ở trang {page}: {e}. Sẽ bỏ qua trang này.")
            continue
            
        soup = BeautifulSoup(response.text, 'html.parser')
        job_titles = soup.find_all('h3', class_='imt-3')
        
        if not job_titles:
            print(f"⚠️ Cảnh báo: Trang {page} trống trơn (Khả năng bị Cloudflare chặn). Dừng vòng lặp.")
            break
            
        for title_elem in job_titles:
            try:
                card = title_elem.parent
                company_elem = card.find('a', class_='text-rich-grey')
                salary_elem = card.find('div', class_='salary')
                skill_elems = card.find_all('a', class_='itag')
                skills = ", ".join([s.text.strip() for s in skill_elems]) if skill_elems else "N/A"

                job_title_text = title_elem.text.strip() if title_elem else "N/A"
                company_text = company_elem.text.strip() if company_elem else "N/A"
                salary_text = salary_elem.text.strip() if salary_elem else "N/A"
                
                # Tạo Khoá băm duy nhất (Unique Hash ID) để UPSERT
                hash_input = f"{job_title_text}_{company_text}"
                job_hash = hashlib.md5(hash_input.encode('utf-8')).hexdigest()

                job = {
                    "job_hash": job_hash,
                    "job_title": job_title_text,
                    "company": company_text,
                    "salary": salary_text,
                    "skills": skills
                }
                extracted_jobs.append(job)
            except Exception as e:
                continue
                
        time.sleep(2)
        
    print(f"🏁 XONG! Tổng cộng đã thu thập được {len(extracted_jobs)} công việc thực tế.")
            
    if not extracted_jobs:
        print("⚠️ Không lấy được thông tin! Sử dụng Mock Data.")
        extracted_jobs = [
            {"job_hash": "mock1", "job_title": "Data Engineer Intern", "company": "Tech Corp", "salary": "700$", "skills": "Python, Airflow"},
            {"job_hash": "mock2", "job_title": "Senior Data Engineer", "company": "VNG", "salary": "2000$", "skills": "Spark, Kafka, AWS"}
        ]

    df = pd.DataFrame(extracted_jobs)
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    object_name = f"raw/jobs_{timestamp}.csv"

    # ==========================================
    # BƯỚC 1: UPLOAD LÊN DATA LAKE (MinIO)
    # ==========================================
    print("⏳ Chuẩn bị đưa dữ liệu thô vào Data Lake (MinIO)...")
    try:
        minio_host = os.environ.get("POSTGRES_HOST", "localhost") 
        # Nếu chạy trong Airflow, gọi qua gateway của Docker network. Chạy ở Windows thì là localhost.
        s3_endpoint = "http://minio:9000" if minio_host == "postgres" else "http://localhost:9000"
        
        s3_client = boto3.client(
            's3',
            endpoint_url=s3_endpoint,
            aws_access_key_id='admin',
            aws_secret_access_key='password'
        )
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False, encoding='utf-8')
        s3_client.put_object(Bucket="it-jobs-lake", Key=object_name, Body=csv_buffer.getvalue().encode('utf-8'))
        print(f"✅ Đã upload thành công lên Data Lake: s3://it-jobs-lake/{object_name}")
    except Exception as e:
        print(f"❌ Lỗi Upload MinIO (Vui lòng kiểm tra container MinIO): {e}")

    # ==========================================
    # BƯỚC 2: UPSERT VÀO DATA WAREHOUSE (PostgreSQL)
    # ==========================================
    print("⏳ Chuẩn bị Increment Load (UPSERT) vào Database PostgreSQL...")
    try:
        from sqlalchemy import create_engine, text
        db_host = os.environ.get("POSTGRES_HOST", "localhost")
        engine = create_engine(f'postgresql://de_user:de_password@{db_host}:5432/job_market')
        
        # Loại bỏ trùng lặp trong CHÍNH batch cào được (Tránh lỗi CardinalityViolation)
        df_deduped = df.drop_duplicates(subset=['job_hash'], keep='first')
        print(f"   ℹ️ Sau khi lọc trùng: {len(df)} → {len(df_deduped)} dòng duy nhất.")
        
        # Đẩy dữ liệu vào bảng tạm (Staging Temp Table)
        df_deduped.to_sql('raw_jobs_temp', con=engine, if_exists='replace', index=False)

        
        # Cập nhật hoặc chèn mới (UPSERT) dựa trên job_hash
        upsert_query = text("""
            INSERT INTO raw_jobs (job_hash, job_title, company, salary, skills, scraped_at)
            SELECT job_hash, job_title, company, salary, skills, CURRENT_TIMESTAMP
            FROM raw_jobs_temp
            ON CONFLICT (job_hash) DO UPDATE 
            SET scraped_at = EXCLUDED.scraped_at,
                salary = EXCLUDED.salary,
                skills = EXCLUDED.skills;
        """)
        
        with engine.begin() as conn:
            conn.execute(upsert_query)
            conn.execute(text("DROP TABLE raw_jobs_temp;"))
            
        print(f"🚀 THÀNH CÔNG! Luồng Incremental Load hoàn tất. Dữ liệu đã hợp nhất vào 'raw_jobs' mà không lo trùng lặp.")
    except Exception as e:
        print(f"❌ Lỗi kết nối DB Upsert: {e}")

if __name__ == "__main__":
    fetch_it_jobs()
