{{ config(
    materialized='incremental',
    unique_key='extract_cdk',
    incremental_strategy='delete+insert',
    on_schema_change='fail'
) }}

WITH base AS (
    SELECT
        extract.extract_cdk,
        extract.work_hk,
        extract.work_id,
        extract._filter_param,
        extract._filter_value AS institution_ror,
        dim_i.institution_hk,
        dim_i.institution_id,
        dim_i.institution_display_name,
        extract.extract_datetime,
        extract.load_datetime,
        extract.source
    FROM {{ ref('fct_openalex_work_extraction') }} extract
    LEFT JOIN {{ ref('dim_openalex_institution') }} dim_i
        ON extract._filter_value = dim_i.ror
    WHERE extract._filter_param = 'institutions.ror'
    {% if is_incremental() %}
      AND extract.load_datetime > (SELECT COALESCE(MAX(load_datetime), '1900-01-01'::timestamp) FROM {{ this }})
    {% endif %}
)

SELECT * FROM base
