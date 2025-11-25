{{ config(materialized = 'table') }}

WITH source AS (
  SELECT * FROM {{ source('oai', 'item_publishers') }}
),

renamed AS (
  SELECT
    "item_id",
    "publishers",
    "extract_datetime",
    "load_datetime"
  FROM source
)

SELECT * FROM renamed
