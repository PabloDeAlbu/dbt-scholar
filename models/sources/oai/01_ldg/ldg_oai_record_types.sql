{{ config(materialized = 'table') }}

WITH source AS (
  SELECT * FROM {{ source('oai', 'record_types') }}
),

renamed AS (
  SELECT
    "record_id",
    "types",
    "extract_datetime",
    "load_datetime"
  FROM source
)

SELECT * FROM renamed
