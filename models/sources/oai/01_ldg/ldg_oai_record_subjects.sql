{{ config(materialized = 'table') }}

WITH source AS (
  SELECT * FROM {{ source('oai', 'record_subjects') }}
),

renamed AS (
  SELECT
    "record_id"::text,
    "subjects"::text,
    "extract_datetime"::timestamp,
    "load_datetime"::timestamp
  FROM source
)

SELECT * FROM renamed
