{{ config(materialized = 'table') }}

WITH source AS (
  SELECT * FROM {{ source('oai', 'item_identifiers') }}
),

renamed AS (
  SELECT
    "item_id",
    "identifiers",
    "extract_datetime",
    "load_datetime"
  FROM source
  WHERE NOT(identifiers = 'CONICET Digital' OR identifiers = 'CONICET')
)

SELECT * FROM renamed
