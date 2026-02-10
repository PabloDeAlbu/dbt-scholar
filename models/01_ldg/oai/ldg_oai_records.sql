{{ config(materialized = 'table') }}

WITH source AS (
  SELECT * FROM {{ source('oai', 'records') }}
),

renamed AS (
  SELECT
    "record_id"::text,
    "col_id"::text,
    "title"::text,
    {{ str_to_date("date_issued") }}::timestamp AS date_issued,
    "_context",
    "extract_datetime"::timestamp,
    "load_datetime"::timestamp
  FROM source
)

SELECT * FROM renamed
