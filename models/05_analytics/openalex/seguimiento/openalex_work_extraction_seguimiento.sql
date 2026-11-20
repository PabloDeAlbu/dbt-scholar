{{ config(materialized='view') }}

WITH base AS (
    SELECT
        extract.extract_cdk,
        extract.work_hk,
        extract.work_id,
        work.title,
        work.publication_year,
        work.publication_date,
        extract._filter_param,
        extract._filter_value,
        extract._filter_value_label,
        extract.extract_datetime,
        extract.load_datetime,
        extract.source
    FROM {{ ref('fct_openalex_work_extraction') }} extract
    LEFT JOIN {{ ref('fct_openalex_work_publication') }} work USING (work_hk)
)

SELECT * FROM base
