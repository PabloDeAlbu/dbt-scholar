{{ config(materialized = 'table') }}

WITH source AS (
    SELECT *
    FROM {{ source('oai', 'sets') }}
),

renamed AS (
    SELECT
        "setSpec"::text AS set_id,
        "setName"::text AS set_name,
        "_source_key"::text,
        "_repository_identifier"::text,
        "_institution_ror"::text,
        "_base_url"::text,
        "_context"::text,
        "_partition_id"::text,
        "extract_datetime"::timestamp AS extract_datetime,
        "load_datetime"::timestamp AS _load_datetime
    FROM source
    WHERE "setSpec" IS NOT NULL
),
ghost_record AS (
    SELECT
        '!UNKNOWN'::text AS set_id,
        '!UNKNOWN'::text AS set_name,
        '!UNKNOWN'::text AS _source_key,
        '!UNKNOWN'::text AS _repository_identifier,
        '!UNKNOWN'::text AS _institution_ror,
        '!UNKNOWN'::text AS _base_url,
        '!UNKNOWN'::text AS _context,
        '!UNKNOWN'::text AS _partition_id,
        '1900-01-01'::timestamp AS extract_datetime,
        '1900-01-01'::timestamp AS _load_datetime
)

SELECT * FROM renamed
UNION ALL
SELECT * FROM ghost_record
