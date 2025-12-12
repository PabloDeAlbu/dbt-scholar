{{ config(materialized = 'table') }}

WITH source AS (
  SELECT * FROM {{ source('oai', 'record_creators') }}
),

renamed AS (
  SELECT
    "record_id"::text,
    "creators"::text as dc_creator,
    "extract_datetime"::timestamp,
    "load_datetime"::timestamp
  FROM source
)

SELECT * FROM renamed
