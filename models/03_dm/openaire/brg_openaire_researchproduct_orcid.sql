WITH base AS (
    SELECT
        researchproduct_orcid_hk,
        researchproduct_hk,
        orcid_hk
    FROM {{ref('link_openaire_researchproduct_orcid')}}
)

SELECT * FROM base
