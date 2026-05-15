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

dependency_map AS (
    SELECT
        dependency_domain,
        dependency_domain_label,
        dependency_name,
        faculty_name
    FROM {{ ref('seed_unlp_scholar_dependency_domain') }}
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
    FROM {{ ref('dim_unlp_scholar_author') }}
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
        END AS dependency_domain,
        CASE
            WHEN author.verified_email_domain IS NULL THEN NULL
            WHEN author.verified_email_domain = 'unlp.edu.ar' THEN 'unlp'
            WHEN author.verified_email_domain ~ '[.]unlp[.]edu[.]ar$'
                THEN REGEXP_REPLACE(
                    SUBSTRING(author.verified_email_domain FROM '([^.]+[.]unlp[.]edu[.]ar)$'),
                    '[.]unlp[.]edu[.]ar$',
                    ''
                )
            ELSE author.verified_email_domain
        END AS dependency_domain_label_derived
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
        COALESCE(dependency_map.dependency_domain_label, author.dependency_domain_label_derived) AS dependency_domain_label,
        COALESCE(
            dependency_map.dependency_name,
            CASE
                WHEN author.dependency_domain IS NULL THEN NULL
                WHEN author.dependency_domain = 'unlp.edu.ar' THEN 'Universidad Nacional de La Plata'
                WHEN author.dependency_domain ~ '[.]unlp[.]edu[.]ar$'
                    THEN INITCAP(REPLACE(REPLACE(author.dependency_domain, '.unlp.edu.ar', ''), '.', ' '))
                ELSE author.dependency_domain
            END
        ) AS dependency_name,
        dependency_map.faculty_name,
        author.has_unlp_verified_email,
        author.mentions_unlp_affiliation,
        author.is_unlp_profile,
        TRUE AS is_primary_affiliation,
        'scholar_org_directory'::text AS affiliation_source,
        author.cited_by_count
    FROM author_with_dependency AS author
    CROSS JOIN organization
    LEFT JOIN dependency_map
        ON author.dependency_domain = dependency_map.dependency_domain
)

SELECT * FROM final
ORDER BY cited_by_count DESC
