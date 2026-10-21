{{ config(materialized = 'table') }}

WITH source AS (
    SELECT *
    FROM {{ source('oai', 'sets') }}
),

renamed AS (
    SELECT
        "setSpec"::text AS set_id,
        "setName"::text AS set_name,
        "extract_datetime"::timestamp AS extract_datetime,
        "load_datetime"::timestamp AS _load_datetime
    FROM source
    WHERE "setSpec" IS NOT NULL
),
ghost_record AS (
    SELECT
        '!UNKNOWN'::text AS set_id,
        '!UNKNOWN'::text AS set_name,
        '1900-01-01'::timestamp AS extract_datetime,
        '1900-01-01'::timestamp AS _load_datetime
)

SELECT * FROM renamed
UNION ALL
SELECT * FROM ghost_record
