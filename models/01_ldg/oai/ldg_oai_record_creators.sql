{{ config(materialized = 'table') }}

WITH source AS (
  SELECT * FROM {{ source('oai', 'record_creators') }}
),

renamed AS (
  SELECT
    "record_id"::text,
    "creators"::text as dc_creator,
    "extract_datetime"::timestamp,
    "dv_load_datetime"::timestamp
  FROM source
),
ghost_record AS (
  SELECT
    '!UNKNOWN'::text as record_id,
    '!UNKNOWN'::text as dc_creator,
    '1900-01-01'::timestamp as extract_datetime,
    {{ dbt_date.today() }} as dv_load_datetime
)

SELECT * FROM renamed
UNION ALL
SELECT * FROM ghost_record
