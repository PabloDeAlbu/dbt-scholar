{{ config(materialized = 'table') }}

WITH base AS (
    SELECT 
        value,
        scheme,
        alternateidentifier_hk,
        source,
        load_datetime
    FROM {{ref('hub_openaire_alternateidentifier')}} hub_altid
)

SELECT * FROM base
