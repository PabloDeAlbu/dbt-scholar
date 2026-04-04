{{ config(materialized='view') }}

WITH base AS (
    SELECT
        extract.work_hk,
        extract._filter_value AS institution_ror,
        dim_work.publication_date,
        dim_i.institution_id,
        dim_i.institution_display_name,
        extract.extract_datetime
    FROM {{ ref('fct_openalex_work_extraction') }} extract
    LEFT JOIN {{ ref('dim_openalex_work') }} dim_work
        USING (work_hk)
    LEFT JOIN {{ ref('dim_openalex_institution') }} dim_i
        ON extract._filter_value = dim_i.ror
    WHERE extract._filter_param = 'institutions.ror'
)

SELECT
    extract_datetime,
    publication_date,
    institution_id,
    institution_ror,
    institution_display_name,
    COUNT(DISTINCT work_hk) AS work_count
FROM base
GROUP BY
    extract_datetime,
    publication_date,
    institution_id,
    institution_ror,
    institution_display_name
ORDER BY extract_datetime DESC
