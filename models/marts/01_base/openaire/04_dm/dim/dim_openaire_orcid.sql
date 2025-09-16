{{ config(materialized = 'table') }}

WITH base AS (
    SELECT 
        orcid_hk,
        orcid,
        load_datetime,
        source
    FROM {{ref('hub_openaire_orcid')}} 
)

SELECT * FROM base