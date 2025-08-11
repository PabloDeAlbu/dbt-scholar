{{ config(materialized = 'table') }}

WITH base as (
    SELECT 
        hub_orcid.orcid_hk,
        hub_orcid.orcid,
        link_author_orcid.author_hk
    FROM {{ref('hub_openaire_orcid')}} hub_orcid
    INNER JOIN {{ref('link_openaire_author_orcid')}} link_author_orcid ON 
        link_author_orcid.orcid_hk = hub_orcid.orcid_hk 
)

SELECT * FROM base