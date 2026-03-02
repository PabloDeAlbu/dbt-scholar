{{ config(materialized = 'table') }}

WITH source AS (
  SELECT * FROM {{ source('oai', 'record_subjects') }}
),

renamed AS (
  SELECT
    "record_id"::text,
    "subjects"::text as dc_subject,
    "extract_datetime"::timestamp,
    "_load_datetime"::timestamp
  FROM source
),
ghost_record AS (
  SELECT
    '!UNKNOWN'::text as record_id,
    '!UNKNOWN'::text as dc_subject,
    '1900-01-01'::timestamp as extract_datetime,
    {{ dbt_date.today() }} as _load_datetime
)

SELECT * FROM renamed
UNION ALL
SELECT * FROM ghost_record
