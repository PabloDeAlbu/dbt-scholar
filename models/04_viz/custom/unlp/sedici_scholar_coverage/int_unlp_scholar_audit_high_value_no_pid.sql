{{ config(materialized='view') }}

SELECT
    'high_value_no_pid'::text AS audit_scenario,
    'has_doi = false and has_handle = true'::text AS cohort_rule,
    base.*
FROM {{ ref('base_unlp_scholar_audit_candidate') }} AS base
WHERE base.has_doi = FALSE
  AND base.has_handle = TRUE
