{{ config(materialized = 'table') }}

WITH source AS (
  SELECT * FROM {{ source('oai', 'record_descriptions') }}
),

renamed AS (
  SELECT
    "record_id"::text,
    "description"::text as dc_description,
    "extract_datetime"::timestamp,
    "load_datetime"::timestamp
  FROM source
)

SELECT * FROM renamed
