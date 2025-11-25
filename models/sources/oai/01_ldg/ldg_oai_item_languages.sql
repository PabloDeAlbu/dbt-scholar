{{ config(materialized = 'table') }}

WITH source AS (
  SELECT * FROM {{ source('oai', 'item_languages') }}
),

renamed AS (
  SELECT
    "item_id",
    "languages",
    "extract_datetime",
    "load_datetime"
  FROM source
)

SELECT * FROM renamed
