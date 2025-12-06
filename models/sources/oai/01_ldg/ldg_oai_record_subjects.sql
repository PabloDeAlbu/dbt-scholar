{{ config(materialized = 'table') }}

WITH source AS (
  SELECT * FROM {{ source('oai', 'record_subjects') }}
),

renamed AS (
  SELECT
    "record_id",
    "subjects",
    "extract_datetime",
    "load_datetime"
  FROM source
)

SELECT * FROM renamed
