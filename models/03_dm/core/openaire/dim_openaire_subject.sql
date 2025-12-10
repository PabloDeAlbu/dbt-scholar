{{ config(materialized = 'table') }}

WITH base AS (
    SELECT 
        subject_scheme,
        subject_value,
        subject_hk
    FROM {{ref('hub_openaire_subject')}} hub_subject
    WHERE subject_scheme != 'keyword'
)

SELECT * FROM base
