{{ config(materialized='table') }}

WITH jobs AS (
    SELECT 
        job_id, 
        raw_skills
    FROM {{ ref('stg_jobs') }}
    WHERE raw_skills IS NOT NULL AND raw_skills != 'N/A'
),

-- PostgreSQL hỗ trợ hàm regexp_split_to_table để tách 1 chuỗi thành nhiều dòng
split_skills AS (
    SELECT 
        job_id,
        TRIM(regexp_split_to_table(raw_skills, ',')) AS skill_name
    FROM jobs
)

-- Thống kê xem mỗi kỹ năng xuất hiện trong bao nhiêu tin tuyển dụng
SELECT 
    skill_name,
    COUNT(DISTINCT job_id) AS job_count
FROM split_skills
WHERE skill_name != ''
GROUP BY skill_name
ORDER BY job_count DESC
