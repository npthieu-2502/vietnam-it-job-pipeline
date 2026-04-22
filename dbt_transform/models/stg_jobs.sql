{{ config(materialized='view') }}

WITH raw_data AS (
    SELECT * FROM raw_jobs
)

SELECT 
    id AS job_id,
    job_hash,
    TRIM(job_title) AS title,
    TRIM(company) AS company_name,
    salary AS raw_salary,
    
    -- Parsing Mức lương (Từ Text sang Số)
    (CASE WHEN salary LIKE '%USD%' THEN TRUE ELSE FALSE END) AS is_usd,
    CAST(NULLIF(regexp_replace(split_part(salary, '-', 1), '[^0-9]', '', 'g'), '') AS INTEGER) AS min_salary_estimate,
    CAST(NULLIF(regexp_replace(split_part(salary, '-', 2), '[^0-9]', '', 'g'), '') AS INTEGER) AS max_salary_estimate,

    -- Làm sạch nhẹ cột Skills
    TRIM(skills) AS raw_skills,
    scraped_at
FROM raw_data
