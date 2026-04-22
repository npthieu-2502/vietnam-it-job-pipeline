-- Khởi tạo bảng chứa dữ liệu thô (Raw Data)
CREATE TABLE IF NOT EXISTS raw_jobs (
    id SERIAL PRIMARY KEY,
    job_title VARCHAR(255),
    company VARCHAR(255),
    salary VARCHAR(100),
    skills TEXT,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
