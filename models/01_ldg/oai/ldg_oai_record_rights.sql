{{ config(materialized = 'table') }}

WITH source AS (
  SELECT * FROM {{ source('oai', 'record_rights') }}
),

renamed AS (
  SELECT
    "record_id"::text,
    "rights"::text as dc_right,
    "extract_datetime"::timestamp,
    "load_datetime"::timestamp
  FROM source
),
ghost_record AS (
  SELECT
    '!UNKNOWN'::text as record_id,
    '!UNKNOWN'::text as dc_right,
    '1900-01-01'::timestamp as extract_datetime,
    {{ dbt_date.today() }} as load_datetime
)

SELECT * FROM renamed
UNION ALL
SELECT * FROM ghost_record
