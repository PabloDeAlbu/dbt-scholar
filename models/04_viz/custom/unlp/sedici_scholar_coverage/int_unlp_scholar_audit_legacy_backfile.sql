{{ config(materialized='view') }}

SELECT
    'legacy_backfile'::text AS audit_scenario,
    'date_accessioned < current_date - interval ''5 years'''::text AS cohort_rule,
    base.*
FROM {{ ref('base_unlp_scholar_audit_candidate') }} AS base
WHERE base.date_accessioned < CURRENT_DATE - INTERVAL '5 years'
