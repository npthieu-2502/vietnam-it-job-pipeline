"""
Script nạp dữ liệu thật từ file CSV (trong data/raw/) vào Database.
Dùng khi ITviec chặn scraping nhưng muốn test hệ thống với data thật.
"""
import pandas as pd
import os
import glob
import hashlib
from sqlalchemy import create_engine, text

def load_from_csv():
    # Tìm tất cả file CSV trong thư mục data/raw/
    data_dir = os.path.join(os.path.dirname(__file__), '../data/raw')
    csv_files = glob.glob(os.path.join(data_dir, '*.csv'))
    
    if not csv_files:
        print("❌ Không tìm thấy file CSV nào trong data/raw/")
        return

    print(f"📂 Tìm thấy {len(csv_files)} file CSV:")
    for f in csv_files:
        print(f"   - {os.path.basename(f)}")

    # Gộp tất cả file CSV lại
    dfs = []
    for f in csv_files:
        try:
            df = pd.read_csv(f, encoding='utf-8')
            dfs.append(df)
            print(f"   ✅ Đọc {os.path.basename(f)}: {len(df)} dòng")
        except Exception as e:
            print(f"   ⚠️  Bỏ qua {os.path.basename(f)}: {e}")

    if not dfs:
        print("❌ Không đọc được file nào.")
        return
    
    df_all = pd.concat(dfs, ignore_index=True)
    print(f"\n📊 Tổng cộng: {len(df_all)} dòng từ tất cả file CSV")

    # Tạo job_hash nếu chưa có
    if 'job_hash' not in df_all.columns:
        print("🔑 Sinh job_hash từ (job_title + company)...")
        df_all['job_hash'] = df_all.apply(
            lambda row: hashlib.md5(
                f"{row.get('job_title','N/A')}_{row.get('company','N/A')}".encode('utf-8')
            ).hexdigest(),
            axis=1
        )

    # Chỉ giữ các cột cần thiết
    cols_needed = ['job_hash', 'job_title', 'company', 'salary', 'skills']
    df_all = df_all[[c for c in cols_needed if c in df_all.columns]]

    # Dedup
    df_deduped = df_all.drop_duplicates(subset=['job_hash'], keep='first')
    print(f"   ℹ️  Sau khi lọc trùng: {len(df_all)} → {len(df_deduped)} dòng duy nhất.")

    # UPSERT vào DB
    print("\n⏳ Nạp dữ liệu vào PostgreSQL...")
    engine = create_engine('postgresql://de_user:de_password@localhost:5432/job_market')
    df_deduped.to_sql('raw_jobs_temp', con=engine, if_exists='replace', index=False)

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

    print(f"🚀 THÀNH CÔNG! Đã nạp {len(df_deduped)} dòng thật vào DB.")
    print("👉 Tiếp theo: Chạy 'dbt run' để xào nấu dữ liệu!")

if __name__ == "__main__":
    load_from_csv()
