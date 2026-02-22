{{ config(materialized = 'table') }}

WITH source AS (
  SELECT * FROM {{ source('oai', 'record_sets') }}
),

renamed AS (
  SELECT
    "record_id"::text,
    "set_id"::text,
    "extract_datetime"::timestamp,
    "dv_load_datetime"::timestamp
  FROM source
),
ghost_record AS (
  SELECT
    '!UNKNOWN'::text as record_id,
    '!UNKNOWN'::text as set_id,
    '1900-01-01'::timestamp as extract_datetime,
    {{ dbt_date.today() }} as dv_load_datetime
)

SELECT * FROM renamed
UNION ALL
SELECT * FROM ghost_record
