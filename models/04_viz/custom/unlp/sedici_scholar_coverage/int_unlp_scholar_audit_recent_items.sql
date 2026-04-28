{{ config(materialized='view') }}

SELECT
    'recent_items'::text AS audit_scenario,
    'date_accessioned desc'::text AS cohort_rule,
    base.*
FROM {{ ref('base_unlp_scholar_audit_candidate') }} AS base
WHERE base.date_accessioned IS NOT NULL
