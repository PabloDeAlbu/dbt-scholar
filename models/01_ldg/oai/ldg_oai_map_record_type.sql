{{ config(materialized = 'table') }}

WITH source AS (
  SELECT * FROM {{ source('oai', 'map_record_type') }}
),

renamed AS (
  SELECT
    "record_id"::text,
    "types"::text as dc_type,
    "extract_datetime"::timestamp,
    "load_datetime"::timestamp as _load_datetime
  FROM source
),
ghost_record AS (
  SELECT
    '!UNKNOWN'::text as record_id,
    '!UNKNOWN'::text as dc_type,
    '1900-01-01'::timestamp as extract_datetime,
    {{ dbt_date.today() }} as _load_datetime
)

SELECT * FROM renamed
UNION ALL
SELECT * FROM ghost_record
