{{ config(
    materialized='incremental',
    unique_key='extract_cdk',
    incremental_strategy='delete+insert',
    on_schema_change='fail'
) }}

WITH base AS (
    SELECT
        sat.extract_cdk,
        record.record_id,
        sat.extract_datetime,
        sat.load_datetime,
        sat.repository_identifier,
        sat.institution_ror,
        sat.source,
        sat.record_hk
    FROM {{ ref('sat_oai_record__extract') }} sat
    INNER JOIN {{ ref('latest_sat_oai_record') }} record USING (record_hk)
    {% if is_incremental() %}
    WHERE sat.load_datetime > (SELECT COALESCE(MAX(load_datetime), '1900-01-01'::timestamp) FROM {{ this }})
    {% endif %}
)

SELECT * FROM base
