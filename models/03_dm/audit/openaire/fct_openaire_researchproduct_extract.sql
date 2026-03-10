{{ config(
    materialized='incremental',
    unique_key='extract_hk',
    incremental_strategy='delete+insert',
    on_schema_change='fail'
) }}

WITH base AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'sat.researchproduct_hk',
            'sat.extract_datetime',
            'sat._filter_param',
            'sat._filter_value'
        ]) }} AS extract_hk,
        dim_rp.researchproduct_id,
        sat._filter_param,
        sat._filter_value,
        sat.extract_datetime,
        sat.load_datetime,
        sat.source,
        dim_rp.researchproduct_hk
    FROM {{ ref('sat_openaire_researchproduct__extract') }} sat
    INNER JOIN {{ ref('dim_openaire_researchproduct') }} dim_rp USING (researchproduct_hk)
    {% if is_incremental() %}
    WHERE sat.load_datetime > (SELECT COALESCE(MAX(load_datetime), '1900-01-01'::timestamp) FROM {{ this }})
    {% endif %}
)

SELECT * FROM base
