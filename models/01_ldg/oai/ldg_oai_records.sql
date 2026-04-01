{{ config(materialized = 'table') }}

WITH source AS (
  SELECT * FROM {{ source('oai', 'records') }}
),

renamed AS (
  SELECT
    "record_id"::text,
    "col_id"::text,
    "title"::text,
    {{ str_to_date("date_issued") }}::timestamp AS date_issued,
    'request'::text AS "_context",
    "extract_datetime"::timestamp,
    "load_datetime"::timestamp AS _load_datetime
  FROM source
),
ghost_record AS (
  SELECT
    '!UNKNOWN'::text as record_id,
    '!UNKNOWN'::text as col_id,
    '!UNKNOWN'::text as title,
    '1900-01-01'::timestamp as date_issued,
    '!UNKNOWN' as "_context",
    '1900-01-01'::timestamp as extract_datetime,
    {{ dbt_date.today() }} as _load_datetime
)

SELECT * FROM renamed
UNION ALL
SELECT * FROM ghost_record
