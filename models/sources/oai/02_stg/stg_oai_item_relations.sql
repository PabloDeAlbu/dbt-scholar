{{ config(materialized = 'table') }}

WITH source AS (
  SELECT * FROM {{ source('oai', 'item_relations') }}
),

renamed AS (
  SELECT
    "item_id",
    "relations",
    "extract_datetime",
    "load_datetime"
  FROM source
)

SELECT * FROM renamed
