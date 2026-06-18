{{ config(materialized = 'table') }}

WITH openaire_organization AS (
    SELECT
        organization_hk AS openaire_organization_hk,
        organization_id AS openaire_organization_id,
        NULLIF(TRIM(organization_ror), '') AS organization_ror,
        acronym AS openaire_acronym,
        legalname AS openaire_legalname,
        organization_name AS openaire_organization_name
    FROM {{ ref('dim_openaire_organization') }}
),

openalex_institution AS (
    SELECT
        institution_hk AS openalex_institution_hk,
        institution_id AS openalex_institution_id,
        institution_display_name AS openalex_institution_name,
        country_code,
        NULLIF(ror, '-') AS organization_ror
    FROM {{ ref('dim_openalex_institution') }}
),

merged AS (
    SELECT
        COALESCE(openaire_organization.organization_ror, openalex_institution.organization_ror) AS organization_ror,
        openaire_organization.openaire_organization_hk,
        openaire_organization.openaire_organization_id,
        openaire_organization.openaire_acronym,
        openaire_organization.openaire_legalname,
        openaire_organization.openaire_organization_name,
        openalex_institution.openalex_institution_hk,
        openalex_institution.openalex_institution_id,
        openalex_institution.openalex_institution_name,
        openalex_institution.country_code,
        openaire_organization.organization_ror IS NOT NULL AS has_openaire,
        openalex_institution.organization_ror IS NOT NULL AS has_openalex
    FROM openaire_organization
    FULL OUTER JOIN openalex_institution
        ON openaire_organization.organization_ror = openalex_institution.organization_ror
),

dim AS (
    SELECT
        organization_ror,
        COALESCE(openaire_organization_name, openalex_institution_name, organization_ror) AS organization_name,
        openaire_organization_hk,
        openaire_organization_id,
        openaire_acronym,
        openaire_legalname,
        openalex_institution_hk,
        openalex_institution_id,
        openalex_institution_name,
        country_code,
        has_openaire,
        has_openalex
    FROM merged
)

SELECT * FROM dim
