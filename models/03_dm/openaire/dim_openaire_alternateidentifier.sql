{{ config(materialized = 'table') }}

WITH base AS (
    SELECT 
        value,
        scheme,
        alternateidentifier_hk,
        source,
        _load_datetime AS load_datetime
    FROM {{ref('hub_openaire_alternateidentifier')}} hub_altid
)

SELECT * FROM base
