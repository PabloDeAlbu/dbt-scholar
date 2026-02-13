{{ config(materialized = 'table') }}

WITH source AS (
    SELECT * 
    FROM {{ source('oai', 'sets') }}
),

renamed AS (
    SELECT 
        "setSpec"::text as set_id,
        "setName"::text as set_name,
        load_datetime::timestamp
    FROM source
),
ghost_record AS (
    SELECT
        '!UNKNOWN'::text as set_id,
        '!UNKNOWN'::text as set_name,
        {{ dbt_date.today() }} as load_datetime
)

SELECT * FROM renamed
UNION ALL
SELECT * FROM ghost_record
