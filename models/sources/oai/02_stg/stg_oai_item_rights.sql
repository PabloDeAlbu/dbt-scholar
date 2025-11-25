{{ config(materialized = 'table') }}

WITH source AS (
  SELECT * FROM {{ source('oai', 'item_rights') }}
),

renamed AS (
  SELECT
    "item_id",
    "rights",
    "extract_datetime",
    "load_datetime"
  FROM source
)

SELECT * FROM renamed
