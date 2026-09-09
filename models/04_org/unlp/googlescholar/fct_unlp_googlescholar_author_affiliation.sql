{{ config(materialized='table') }}

{%- set unlp_ror = 'https://ror.org/01tjs6929' -%}
{%- set scholar_org_id = '18050997564342690566' -%}
{%- set scholar_org_profile_url = 'https://scholar.google.com/citations?view_op=view_org&hl=es&org=18050997564342690566' -%}

WITH organization AS (
    SELECT
        organization_ror,
        organization_name,
        openaire_organization_id,
        openalex_institution_id,
        has_openaire,
        has_openalex
    FROM {{ ref('dim_organization') }}
    WHERE organization_ror = '{{ unlp_ror }}'
),

organizational_unit AS (
    SELECT
        organizational_unit_domain,
        organizational_unit_domain_label,
        organizational_unit_name,
        parent_organizational_unit_name
    FROM {{ ref('dim_unlp_organizational_unit') }}
),

author AS (
    SELECT
        scholar_user_id,
        author_name,
        profile_url,
        affiliation,
        verified_email_domain,
        has_unlp_verified_email,
        mentions_unlp_affiliation,
        is_unlp_profile,
        COALESCE(cited_by_count::int, 0) AS cited_by_count
    FROM {{ ref('dim_unlp_googlescholar_author') }}
),

author_with_dependency AS (
    SELECT
        author.*,
        CASE
            WHEN author.verified_email_domain IS NULL THEN NULL
            WHEN author.verified_email_domain = 'unlp.edu.ar' THEN 'unlp.edu.ar'
            WHEN author.verified_email_domain ~ '[.]unlp[.]edu[.]ar$'
                THEN SUBSTRING(author.verified_email_domain FROM '([^.]+[.]unlp[.]edu[.]ar)$')
            ELSE author.verified_email_domain
        END AS dependency_domain
    FROM author
),

final AS (
    SELECT
        author.scholar_user_id,
        author.author_name,
        author.profile_url,
        author.affiliation AS scholar_affiliation,
        '{{ scholar_org_id }}'::text AS scholar_org_id,
        '{{ scholar_org_profile_url }}'::text AS scholar_org_profile_url,
        organization.organization_ror,
        organization.organization_name,
        organization.openaire_organization_id,
        organization.openalex_institution_id,
        organization.has_openaire,
        organization.has_openalex,
        author.verified_email_domain,
        author.dependency_domain,
        organizational_unit.organizational_unit_domain_label AS dependency_domain_label,
        organizational_unit.organizational_unit_name AS dependency_name,
        organizational_unit.parent_organizational_unit_name AS parent_unit_name,
        author.has_unlp_verified_email,
        author.mentions_unlp_affiliation,
        author.is_unlp_profile,
        TRUE AS is_primary_affiliation,
        'scholar_org_directory'::text AS affiliation_source,
        author.cited_by_count
    FROM author_with_dependency AS author
    CROSS JOIN organization
    LEFT JOIN organizational_unit
        ON author.dependency_domain = organizational_unit.organizational_unit_domain
)

SELECT * FROM final
ORDER BY cited_by_count DESC
