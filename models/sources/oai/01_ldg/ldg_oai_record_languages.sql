{{ config(materialized = 'table') }}

WITH source AS (
  SELECT * FROM {{ source('oai', 'record_languages') }}
),

renamed AS (
  SELECT
    "record_id"::text,
    "languages"::text,
    "extract_datetime"::timestamp,
    "load_datetime"::timestamp
  FROM source
)

SELECT * FROM renamed
