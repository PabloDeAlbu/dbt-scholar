{{ config(materialized = 'table') }}

WITH source AS (
  SELECT * FROM {{ source('oai', 'records') }}
),

renamed AS (
  SELECT
    "record_id",
    "col_id",
    "title",
    "date_issued",
    {# "types",
    "identifiers",
    "languages",
    "subjects",
    "publishers",
    "relations",
    "rights", #}
    "extract_datetime",
    "load_datetime"
  FROM source
)

SELECT * FROM renamed
