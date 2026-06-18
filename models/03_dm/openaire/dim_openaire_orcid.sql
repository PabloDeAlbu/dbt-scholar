{{ config(materialized = 'table') }}

WITH base AS (
    SELECT 
        orcid_hk,
        orcid,
        _load_datetime AS load_datetime,
        source
    FROM {{ref('hub_openaire_orcid')}} 
)

SELECT * FROM base
