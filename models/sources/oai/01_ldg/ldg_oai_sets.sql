{{ config(materialized = 'table') }}

WITH source AS (
    SELECT * 
    FROM {{ source('oai', 'sets') }}
),

renamed AS (
    SELECT 
        "setSpec"::text as set_id,
        "setName"::text as name,
        load_datetime::timestamp
    FROM source
)

SELECT * FROM renamed
