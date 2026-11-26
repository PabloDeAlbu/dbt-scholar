{{ config(materialized='table', schema='core') }}

-- ¿Qué relaciones ROR informa OpenAIRE para sus organizaciones?
WITH openaire_organization_ror AS (
    SELECT DISTINCT
        organization_hk,
        NULLIF(value, '') AS organization_ror
    FROM {{ ref('brg_openaire_organization_pid') }}
    WHERE LOWER(scheme) = 'ror'
      AND value != '!UNKNOWN'
),

-- ¿Qué organizaciones de OpenAIRE tienen un único ROR?
openaire_organization_ror_count AS (
    SELECT
        organization_hk,
        COUNT(DISTINCT organization_ror) AS ror_count
    FROM openaire_organization_ror
    GROUP BY organization_hk
),

-- ¿Qué ROR identifican una única organización de OpenAIRE?
openaire_ror_organization_count AS (
    SELECT
        organization_ror,
        COUNT(DISTINCT organization_hk) AS organization_count
    FROM openaire_organization_ror
    GROUP BY organization_ror
),

-- ¿Qué relaciones organización–ROR son unívocas en ambos sentidos en OpenAIRE?
openaire_organization_ror_unambiguous AS (
    SELECT
        relationship.organization_hk,
        relationship.organization_ror
    FROM openaire_organization_ror relationship
    INNER JOIN openaire_organization_ror_count organization_count
        USING (organization_hk)
    INNER JOIN openaire_ror_organization_count ror_count
        USING (organization_ror)
    WHERE organization_count.ror_count = 1
      AND ror_count.organization_count = 1
),

-- ¿Qué atributos y ROR no ambiguo aporta cada organización de OpenAIRE?
openaire_organization AS (
    SELECT
        organization.organization_hk AS openaire_organization_hk,
        organization.organization_id AS openaire_organization_id,
        organization_ror.organization_ror,
        organization.acronym AS openaire_acronym,
        organization.legalname AS openaire_legalname,
        organization.organization_name AS openaire_organization_name
    FROM {{ ref('dim_openaire_organization') }} organization
    LEFT JOIN openaire_organization_ror_unambiguous organization_ror
        USING (organization_hk)
),

-- ¿Qué atributos y ROR aporta cada institución de OpenAlex?
openalex_institution AS (
    SELECT
        institution_hk AS openalex_institution_hk,
        institution_id AS openalex_institution_id,
        institution_display_name AS openalex_institution_name,
        country_code,
        NULLIF(ror, '-') AS organization_ror
    FROM {{ ref('dim_openalex_institution') }}
),

-- ¿Qué ROR identifican una única institución de OpenAlex?
openalex_ror_institution_count AS (
    SELECT
        organization_ror,
        COUNT(DISTINCT openalex_institution_hk) AS institution_count
    FROM openalex_institution
    WHERE organization_ror IS NOT NULL
    GROUP BY organization_ror
),

-- ¿Qué clave de cruce evita unir un ROR compartido por varias instituciones de OpenAlex?
openalex_institution_matchable AS (
    SELECT
        institution.*,
        CASE
            WHEN ror_count.institution_count = 1 THEN institution.organization_ror
        END AS organization_match_ror
    FROM openalex_institution institution
    LEFT JOIN openalex_ror_institution_count ror_count
        USING (organization_ror)
),

-- ¿Qué organizaciones de OpenAIRE y OpenAlex pueden consolidarse mediante un ROR no ambiguo?
merged AS (
    SELECT
        COALESCE(openaire_organization.organization_ror, openalex_institution.organization_match_ror) AS organization_ror,
        openaire_organization.openaire_organization_hk,
        openaire_organization.openaire_organization_id,
        openaire_organization.openaire_acronym,
        openaire_organization.openaire_legalname,
        openaire_organization.openaire_organization_name,
        openalex_institution.openalex_institution_hk,
        openalex_institution.openalex_institution_id,
        openalex_institution.openalex_institution_name,
        openalex_institution.country_code,
        openaire_organization.openaire_organization_hk IS NOT NULL AS has_openaire,
        openalex_institution.openalex_institution_hk IS NOT NULL AS has_openalex
    FROM openaire_organization
    FULL OUTER JOIN openalex_institution_matchable openalex_institution
        ON openaire_organization.organization_ror = openalex_institution.organization_match_ror
),

-- ¿Qué atributos expone dim_organization?
final AS (
    SELECT
        organization_ror,
        COALESCE(openaire_organization_name, openalex_institution_name) AS organization_name,
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

SELECT *
FROM final
