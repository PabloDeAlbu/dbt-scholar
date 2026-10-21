{{ config(materialized = 'table') }}

WITH source AS (
  SELECT * FROM {{ source('oai', 'map_record_set') }}
),

record_context AS (
  SELECT
    "record_id"::text AS record_id,
    "extract_datetime"::timestamp AS extract_datetime,
    MIN("_context"::text) AS _context,
    MIN("_source_key"::text) AS _source_key,
    MIN("_repository_identifier"::text) AS _repository_identifier,
    MIN("_institution_ror"::text) AS _institution_ror,
    MIN("_base_url"::text) AS _base_url,
    MIN("_metadata_prefix"::text) AS _metadata_prefix
  FROM {{ source('oai', 'records') }}
  WHERE "record_id" IS NOT NULL
  GROUP BY "record_id"::text, "extract_datetime"::timestamp
  HAVING COUNT(DISTINCT "_repository_identifier"::text) = 1
),

renamed AS (
  SELECT
    source."record_id"::text,
    source."set_id"::text,
    context._context,
    context._source_key,
    context._repository_identifier,
    context._institution_ror,
    context._base_url,
    context._metadata_prefix,
    source."extract_datetime"::timestamp,
    source."load_datetime"::timestamp AS _load_datetime
  FROM source
  LEFT JOIN record_context AS context
    ON source."record_id" = context.record_id
   AND source."extract_datetime"::timestamp = context.extract_datetime
),
ghost_record AS (
  SELECT
    '!UNKNOWN'::text AS record_id,
    '!UNKNOWN'::text AS set_id,
    '!UNKNOWN'::text AS _context,
    '!UNKNOWN'::text AS _source_key,
    '!UNKNOWN'::text AS _repository_identifier,
    '!UNKNOWN'::text AS _institution_ror,
    '!UNKNOWN'::text AS _base_url,
    '!UNKNOWN'::text AS _metadata_prefix,
    '1900-01-01'::timestamp AS extract_datetime,
    '1900-01-01'::timestamp AS _load_datetime
)

SELECT * FROM renamed
UNION ALL
SELECT * FROM ghost_record
