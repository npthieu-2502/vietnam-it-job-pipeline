{{ config(materialized='view') }}

WITH base_jobs AS (
    SELECT *
    FROM {{ ref('stg_jobs') }}
),

categorized AS (
    SELECT
        job_id,
        job_hash,
        title,
        company_name,
        min_salary_estimate,
        max_salary_estimate,
        
        -- Phân loại Cấp Bậc (Level)
        CASE 
            WHEN title ILIKE '%intern%' OR title ILIKE '%thực tập%' THEN 'Intern'
            WHEN title ILIKE '%fresher%' OR title ILIKE '%trainee%' THEN 'Fresher'
            WHEN title ILIKE '%junior%' OR title ILIKE '% jr%' OR title ILIKE 'jr %' THEN 'Junior'
            WHEN title ILIKE '%senior%' OR title ILIKE '% sr%' OR title ILIKE 'sr %' THEN 'Senior'
            WHEN title ILIKE '%lead%' OR title ILIKE '%principal%' THEN 'Lead'
            WHEN title ILIKE '%manager%' OR title ILIKE '%director%' OR title ILIKE '%head%' THEN 'Manager'
            ELSE 'All Levels'
        END AS job_level,

        -- Phân loại Nhóm ngành (Role)
        CASE
            WHEN title ILIKE '%data engineer%' OR title ILIKE '% de %' OR title ILIKE 'de %' OR title ILIKE '%big data%' THEN 'Data Engineer'
            WHEN title ILIKE '%data analyst%' OR title ILIKE '% da %' OR title ILIKE 'da %' OR title ILIKE '%business analyst%' OR title ILIKE '% ba %' OR title ILIKE 'ba %' THEN 'Data Analyst / BA'
            WHEN title ILIKE '%data scientist%' OR title ILIKE '% ai %' OR title ILIKE 'ai %' OR title ILIKE '% ml %' OR title ILIKE '%machine learning%' THEN 'Data Scientist / AI'
            WHEN title ILIKE '%backend%' OR title ILIKE '%back-end%' OR title ILIKE '%java %' OR title ILIKE 'java %' OR title ILIKE '%node%' OR title ILIKE '%.net%' OR title ILIKE '%php%' THEN 'Backend Engineer'
            WHEN title ILIKE '%frontend%' OR title ILIKE '%front-end%' OR title ILIKE '%react%' OR title ILIKE '%vue%' OR title ILIKE '%angular%' THEN 'Frontend Engineer'
            WHEN title ILIKE '%fullstack%' OR title ILIKE '%full-stack%' THEN 'Fullstack Engineer'
            WHEN title ILIKE '%mobile%' OR title ILIKE '%ios%' OR title ILIKE '%android%' OR title ILIKE '%flutter%' THEN 'Mobile Developer'
            WHEN title ILIKE '%devops%' OR title ILIKE '%cloud%' OR title ILIKE '%aws%' OR title ILIKE '%sysadmin%' OR title ILIKE '%system%' THEN 'DevOps / Cloud'
            WHEN title ILIKE '%qa %' OR title ILIKE 'qa %' OR title ILIKE '% qc %' OR title ILIKE 'qc %' OR title ILIKE '%tester%' OR title ILIKE '%automation%' THEN 'QA / QC / Tester'
            WHEN title ILIKE '%product manager%' OR title ILIKE '% po %' OR title ILIKE 'po %' OR title ILIKE '%scrum master%' THEN 'Product Manager / PO'
            WHEN title ILIKE '%ui%' OR title ILIKE '%ux%' OR title ILIKE '%design%' THEN 'UI/UX Designer'
            ELSE 'Software Engineer / Other'
        END AS job_role

    FROM base_jobs
)

SELECT * FROM categorized
