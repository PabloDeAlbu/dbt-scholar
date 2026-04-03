{{ config(
    materialized='incremental',
    unique_key='extract_cdk',
    incremental_strategy='delete+insert',
    on_schema_change='fail'
) }}

WITH base AS (
    SELECT
        sat.extract_cdk,
        hub.work_id,
        sat._filter_param,
        sat._filter_value,
        sat.extract_datetime,
        sat.load_datetime,
        sat.source,
        sat.work_hk
    FROM {{ ref('sat_openalex_work__extract') }} sat
    INNER JOIN {{ ref('hub_openalex_work') }} hub USING (work_hk)
    {% if is_incremental() %}
    WHERE sat.load_datetime > (SELECT COALESCE(MAX(load_datetime), '1900-01-01'::timestamp) FROM {{ this }})
    {% endif %}
)

SELECT * FROM base
