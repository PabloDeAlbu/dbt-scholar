{{ config(materialized = 'table') }}

WITH source AS (
    SELECT *
    FROM {{ source('oai', 'map_identifier_set') }}
),

renamed AS (
    SELECT
        "set_id"::text as set_id,
        "set_id"::text as set_name,
        max("_load_datetime"::timestamp) as _load_datetime
    FROM source
    WHERE "set_id" IS NOT NULL
    GROUP BY "set_id"
),
ghost_record AS (
    SELECT
        '!UNKNOWN'::text as set_id,
        '!UNKNOWN'::text as set_name,
        {{ dbt_date.today() }} as _load_datetime
)

SELECT * FROM renamed
UNION ALL
SELECT * FROM ghost_record
