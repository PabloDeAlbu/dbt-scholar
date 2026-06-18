{{ config(materialized = 'table') }}

WITH base AS (
    SELECT
        seed.type AS work_type,
        seed.label AS work_type_label,
        seed.label_es AS work_type_label_es,
        seed.vocabulary_url AS work_type_vocabulary_url,
        seed.coar_uri AS resource_type_uri,
        coar.label AS resource_type_label,
        coar.label_es AS resource_type_label_es,
        seed.sort_order,
        seed.is_controlled_vocabulary
    FROM {{ ref('seed_openalex_work_type') }} seed
    LEFT JOIN {{ ref('dim_coar_resource_type') }} coar
        ON seed.coar_uri = coar.coar_uri
)

SELECT * FROM base
