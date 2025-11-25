{{ config(materialized = 'table') }}

WITH source AS (
  SELECT * FROM {{ source('oai', 'item_subjects') }}
),

renamed AS (
  SELECT
    "item_id",
    "subjects",
    "extract_datetime",
    "load_datetime"
  FROM source
)

SELECT * FROM renamed
