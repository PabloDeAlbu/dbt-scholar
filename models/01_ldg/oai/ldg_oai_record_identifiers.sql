{{ config(materialized = 'table') }}

WITH source AS (
  SELECT * FROM {{ source('oai', 'record_identifiers') }}
),

renamed AS (
  SELECT
    "record_id"::text,
    "identifiers"::text as dc_identifier,
    "extract_datetime"::timestamp,
    "load_datetime"::timestamp
  FROM source
  WHERE NOT(identifiers = 'CONICET Digital' OR identifiers = 'CONICET')
)

SELECT * FROM renamed
