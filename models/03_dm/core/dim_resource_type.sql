{{ config(materialized = 'table') }}

WITH base AS (
    SELECT
        coar_uri AS resource_type_uri,
        label AS resource_type_label,
        label_es AS resource_type_label_es,
        parent_label_1,
        parent_label_2,
        parent_label_3
    FROM {{ ref('dim_coar_resource_type') }}
)

SELECT * FROM base
