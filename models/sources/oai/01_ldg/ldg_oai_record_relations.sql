{{ config(materialized = 'table') }}

WITH source AS (
  SELECT * FROM {{ source('oai', 'record_relations') }}
),

renamed AS (
  SELECT
    "record_id",
    "relations",
    "extract_datetime",
    "load_datetime"
  FROM source
)

SELECT * FROM renamed
