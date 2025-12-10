{{ config(materialized = 'table') }}

WITH source AS (
  SELECT * FROM {{ source('oai', 'record_relations') }}
),

renamed AS (
  SELECT
    "record_id"::text,
    "relations"::text as dc_relation,
    "extract_datetime"::timestamp,
    "load_datetime"::timestamp
  FROM source
)

SELECT * FROM renamed
