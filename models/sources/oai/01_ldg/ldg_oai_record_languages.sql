{{ config(materialized = 'table') }}

WITH source AS (
  SELECT * FROM {{ source('oai', 'record_languages') }}
),

renamed AS (
  SELECT
    "record_id",
    "languages",
    "extract_datetime",
    "load_datetime"
  FROM source
)

SELECT * FROM renamed
