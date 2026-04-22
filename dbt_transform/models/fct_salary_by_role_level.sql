{{ config(materialized='table') }}

WITH categorized_jobs AS (
    SELECT *
    FROM {{ ref('int_jobs_categorized') }}
),

salary_stats AS (
    SELECT 
        job_role,
        job_level,
        COUNT(job_id) AS total_jobs,
        ROUND(AVG(min_salary_estimate), 0) AS avg_min_salary_usd,
        ROUND(AVG(max_salary_estimate), 0) AS avg_max_salary_usd,
        MAX(max_salary_estimate) AS highest_salary_offered_usd
    FROM categorized_jobs
    -- Chỉ tính những Job có đề cập lương rành mạch
    WHERE min_salary_estimate IS NOT NULL 
    GROUP BY job_role, job_level
)

SELECT * 
FROM salary_stats
ORDER BY job_role, job_level
