{{ config(materialized='view') }}

WITH base AS (
    SELECT
        id::text AS researchproduct_id,
        _filter_param::text AS _filter_param,
        _filter_value::text AS _filter_value,
        _load_datetime::timestamp AS load_datetime,
        _extract_datetime::timestamp AS extract_datetime
    FROM {{ source('openaire', 'researchproduct') }}
),

agg AS (
    SELECT
        researchproduct_id,
        extract_datetime,
        _filter_param,
        _filter_value,
        load_datetime,
        COUNT(*) AS row_count
    FROM base
    GROUP BY
        researchproduct_id,
        extract_datetime,
        _filter_param,
        _filter_value,
        load_datetime
)

SELECT *
FROM agg
WHERE row_count > 1

