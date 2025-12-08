{{ config(materialized = 'table') }}

WITH source AS (
  SELECT * FROM {{ source('oai', 'record_rights') }}
),

renamed AS (
  SELECT
    "record_id"::text,
    "rights"::text,
    "extract_datetime"::timestamp,
    "load_datetime"::timestamp
  FROM source
)

SELECT * FROM renamed
