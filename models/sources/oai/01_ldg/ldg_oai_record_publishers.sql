{{ config(materialized = 'table') }}

WITH source AS (
  SELECT * FROM {{ source('oai', 'record_publishers') }}
),

renamed AS (
  SELECT
    "record_id"::text,
    "publishers"::text,
    "extract_datetime"::timestamp,
    "load_datetime"::timestamp
  FROM source
)

SELECT * FROM renamed
