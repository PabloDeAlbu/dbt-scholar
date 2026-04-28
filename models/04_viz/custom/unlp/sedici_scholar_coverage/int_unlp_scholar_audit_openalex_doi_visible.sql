{{ config(materialized='view') }}

SELECT
    'openalex_doi_visible'::text AS audit_scenario,
    'has_doi = true and matched_by_unique_doi = true'::text AS cohort_rule,
    base.*
FROM {{ ref('base_unlp_scholar_audit_candidate') }} AS base
WHERE base.has_doi = TRUE
  AND base.matched_by_unique_doi = TRUE
