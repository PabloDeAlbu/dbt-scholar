{{ config(materialized = 'table') }}

WITH source AS (
  SELECT * FROM {{ source('oai', 'record_identifiers') }}
),

renamed AS (
  SELECT
    "record_id"::text,
    "identifiers"::text as dc_identifier,
    "extract_datetime"::timestamp,
    "_load_datetime"::timestamp
  FROM source
  WHERE NOT(identifiers = 'CONICET Digital' OR identifiers = 'CONICET')
),
ghost_record AS (
  SELECT
    '!UNKNOWN'::text as record_id,
    '!UNKNOWN'::text as dc_identifier,
    '1900-01-01'::timestamp as extract_datetime,
    {{ dbt_date.today() }} as _load_datetime
)

SELECT * FROM renamed
UNION ALL
SELECT * FROM ghost_record
