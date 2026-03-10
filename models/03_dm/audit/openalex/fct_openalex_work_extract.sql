{{ config(
    materialized='incremental',
    unique_key='extract_hk',
    incremental_strategy='delete+insert',
    on_schema_change='fail'
) }}

WITH base AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'sat.work_hk',
            'sat.extract_datetime',
            'sat._filter_param',
            'sat._filter_value'
        ]) }} AS extract_hk,
        dim_w.work_id,
        sat._filter_param,
        sat._filter_value,
        sat.extract_datetime,
        sat.load_datetime,
        sat.source,
        dim_w.work_hk
    FROM {{ ref('sat_openalex_work__extract') }} sat
    INNER JOIN {{ ref('dim_openalex_work') }} dim_w USING (work_hk)
    {% if is_incremental() %}
    WHERE sat.load_datetime > (SELECT COALESCE(MAX(load_datetime), '1900-01-01'::timestamp) FROM {{ this }})
    {% endif %}
)

SELECT * FROM base
