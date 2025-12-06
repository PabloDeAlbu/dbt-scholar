{{ config(materialized = 'table') }}

WITH source AS (
  SELECT * FROM {{ source('oai', 'record_identifiers') }}
),

renamed AS (
  SELECT
    "record_id",
    "identifiers",
    "extract_datetime",
    "load_datetime"
  FROM source
  WHERE NOT(identifiers = 'CONICET Digital' OR identifiers = 'CONICET')
)

SELECT * FROM renamed
