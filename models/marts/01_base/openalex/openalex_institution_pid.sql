WITH base AS (
    SELECT 
        institution.institution_hk,
        institution.institution_id,
        hub_ror.ror
    FROM {{ref('openalex_institution')}} institution 
    LEFT JOIN {{ref('link_openalex_institution_ror')}} link_institution_ror USING (institution_hk)
    LEFT JOIN {{ref('hub_openalex_ror')}} hub_ror USING (ror_hk)
)

SELECT * FROM base
