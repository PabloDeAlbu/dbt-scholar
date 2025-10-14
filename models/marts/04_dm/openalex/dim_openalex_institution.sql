{{ config(materialized = 'table') }}

WITH institution AS (
    SELECT
        hub_institution.institution_id,
        hub_institution.institution_hk,
        COALESCE(sat_institution.display_name, sat_institution_dehydrated.display_name) AS institution_display_name,
        COALESCE(sat_institution.country_code, '-') AS country_code
    FROM {{ref('hub_openalex_institution')}} hub_institution
    LEFT JOIN {{ref('sat_openalex_institution')}} sat_institution USING (institution_hk)
    LEFT JOIN {{ref('sat_openalex_institution_dehydrated')}} sat_institution_dehydrated USING (institution_hk)
),

institution_ror AS (
    SELECT
        link_institution_ror.institution_hk,
        MAX(hub_ror.ror) AS ror
    FROM {{ref('link_openalex_institution_ror')}} link_institution_ror
    LEFT JOIN {{ref('hub_openalex_ror')}} hub_ror USING (ror_hk)
    GROUP BY link_institution_ror.institution_hk
),

dim AS (
    SELECT
        institution.institution_id,
        institution.institution_hk,
        institution.institution_display_name,
        institution.country_code,
        COALESCE(institution_ror.ror, '-') AS ror,
        CASE WHEN COALESCE(institution_ror.ror, '-') <> '-' THEN TRUE ELSE FALSE END AS has_ror
    FROM institution
    LEFT JOIN institution_ror USING (institution_hk)
)

SELECT * FROM dim
