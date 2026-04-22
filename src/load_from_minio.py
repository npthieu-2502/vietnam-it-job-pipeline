"""
Script kéo TẤT CẢ file CSV từ MinIO Data Lake về và nạp vào PostgreSQL.
Dùng khi không scrape được web nhưng MinIO vẫn đang lưu data thật.
"""
import pandas as pd
import hashlib
import io
import boto3
from sqlalchemy import create_engine, text

MINIO_ENDPOINT   = "http://localhost:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password"
BUCKET_NAME      = "it-jobs-lake"

def load_from_minio():
    print("[INFO] Ket noi MinIO Data Lake...")
    s3 = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

    # Liệt kê toàn bộ file CSV trong bucket
    objects = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix="raw/")
    files = [o['Key'] for o in objects.get('Contents', []) if o['Key'].endswith('.csv')]

    if not files:
        print("❌ Không tìm thấy file nào trong MinIO bucket!")
        return

    print(f"📂 Tìm thấy {len(files)} file CSV trong MinIO:")
    for f in files:
        print(f"   - {f}")

    # Đọc và gộp tất cả file
    dfs = []
    for key in files:
        try:
            obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
            df  = pd.read_csv(io.BytesIO(obj['Body'].read()), encoding='utf-8')
            dfs.append(df)
            print(f"   ✅ {key}: {len(df)} dòng")
        except Exception as e:
            print(f"   ⚠️  Bỏ qua {key}: {e}")

    if not dfs:
        print("❌ Không đọc được file nào.")
        return

    df_all = pd.concat(dfs, ignore_index=True)
    print(f"\n📊 Tổng cộng: {len(df_all)} dòng từ MinIO")

    # Tạo job_hash nếu chưa có
    if 'job_hash' not in df_all.columns:
        print("🔑 Sinh job_hash...")
        df_all['job_hash'] = df_all.apply(
            lambda r: hashlib.md5(
                f"{r.get('job_title','N/A')}_{r.get('company','N/A')}".encode('utf-8')
            ).hexdigest(), axis=1
        )

    # Dedup
    df_deduped = df_all.drop_duplicates(subset=['job_hash'], keep='first')
    print(f"   ℹ️  Sau khi lọc trùng: {len(df_all)} → {len(df_deduped)} dòng duy nhất.")

    # UPSERT vào PostgreSQL
    print("\n⏳ Nạp dữ liệu vào PostgreSQL...")
    engine = create_engine('postgresql://de_user:de_password@localhost:5432/job_market')
    cols   = ['job_hash','job_title','company','salary','skills']
    df_deduped[[c for c in cols if c in df_deduped.columns]] \
        .to_sql('raw_jobs_temp', con=engine, if_exists='replace', index=False)

    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO raw_jobs (job_hash, job_title, company, salary, skills, scraped_at)
            SELECT job_hash, job_title, company, salary, skills, CURRENT_TIMESTAMP
            FROM raw_jobs_temp
            ON CONFLICT (job_hash) DO UPDATE
            SET scraped_at = EXCLUDED.scraped_at,
                salary     = EXCLUDED.salary,
                skills     = EXCLUDED.skills;
        """))
        conn.execute(text("DROP TABLE raw_jobs_temp;"))

    print(f"🚀 THÀNH CÔNG! Đã nạp {len(df_deduped)} dòng vào DB từ MinIO.")
    print("👉 Tiếp theo: Chạy 'dbt run --profiles-dir .' để xào nấu dữ liệu!")

if __name__ == "__main__":
    load_from_minio()
