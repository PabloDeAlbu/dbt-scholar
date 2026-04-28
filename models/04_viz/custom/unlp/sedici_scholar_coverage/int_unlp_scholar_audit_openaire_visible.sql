{{ config(materialized='view') }}

SELECT
    'openaire_visible'::text AS audit_scenario,
    'matched_by_unique_original_id = true'::text AS cohort_rule,
    base.*
FROM {{ ref('base_unlp_scholar_audit_candidate') }} AS base
WHERE base.matched_by_unique_original_id = TRUE
