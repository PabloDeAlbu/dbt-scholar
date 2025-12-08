WITH base AS (
    SELECT 
        hub_subject.subjects,
        hub_subject.subject_hk
    FROM {{ ref('hub_oai_subject') }} hub_subject
)

SELECT * FROM base
