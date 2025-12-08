WITH base AS (
    SELECT 
        hub_record.record_id,
        hub_record.record_hk
    FROM {{ ref('hub_oai_record') }} hub_record
)

SELECT * FROM base
