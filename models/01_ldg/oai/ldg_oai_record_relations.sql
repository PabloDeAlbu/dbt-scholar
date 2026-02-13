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
),
ghost_record AS (
  SELECT
    '!UNKNOWN'::text as record_id,
    '!UNKNOWN'::text as dc_relation,
    '1900-01-01'::timestamp as extract_datetime,
    {{ dbt_date.today() }} as load_datetime
)

SELECT * FROM renamed
UNION ALL
SELECT * FROM ghost_record
