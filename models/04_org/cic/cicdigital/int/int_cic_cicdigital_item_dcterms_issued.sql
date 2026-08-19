{{ config(materialized='view') }}

WITH parsed AS (
    SELECT
        item_id,
        metadata_value_id,
        value_raw,
        CASE
            WHEN value ~ '^[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])$'
                THEN TO_DATE(value, 'YYYY-MM-DD')
            WHEN value ~ '^[0-9]{4}-(0[1-9]|1[0-2])$'
                THEN TO_DATE(value || '-01', 'YYYY-MM-DD')
            WHEN value ~ '^[0-9]{4}$'
                THEN TO_DATE(value || '-01-01', 'YYYY-MM-DD')
        END AS value,
        CASE
            WHEN value ~ '^[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])$' THEN 'day'
            WHEN value ~ '^[0-9]{4}-(0[1-9]|1[0-2])$' THEN 'month'
            WHEN value ~ '^[0-9]{4}$' THEN 'year'
        END AS value_precision
    FROM {{ ref('int_cic_cicdigital_item_metadatavalue') }}
    WHERE metadatafield_fullname = 'dcterms.issued'
),

ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY item_id ORDER BY metadata_value_id DESC) AS value_rank
    FROM parsed
    WHERE value BETWEEN '1500-01-01'::date AND '2100-12-31'::date
)

SELECT item_id, metadata_value_id, value_raw, value, value_precision
FROM ranked
WHERE value_rank = 1
