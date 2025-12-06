{{ config(materialized = 'table') }}

WITH source AS (
  SELECT * FROM {{ source('oai', 'record_publishers') }}
),

renamed AS (
  SELECT
    "record_id",
    "publishers",
    "extract_datetime",
    "load_datetime"
  FROM source
)

SELECT * FROM renamed
