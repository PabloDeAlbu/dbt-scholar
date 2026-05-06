{{ config(materialized = 'table') }}

WITH base AS (
    SELECT
        type AS researchproduct_type,
        label AS researchproduct_type_label,
        vocabulary_code,
        vocabulary_url,
        term_url AS researchproduct_type_uri,
        sort_order,
        is_controlled_vocabulary
    FROM {{ ref('seed_openaire_researchproduct_type') }}
)

SELECT * FROM base
