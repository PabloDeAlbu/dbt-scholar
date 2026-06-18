{{ config(materialized='table') }}

WITH distinct_type AS (
    SELECT DISTINCT
        type AS text_value
    FROM {{ ref('fct_unlp_sedici_item_publication') }}
    WHERE NULLIF(TRIM(type), '') IS NOT NULL
),

typed AS (
    SELECT
        dt.text_value,
        seed.label_es AS resource_type_label_es,
        seed.coar_uri AS resource_type_uri,
        (seed.type IS NOT NULL) AS matches_seed_mapping
    FROM distinct_type AS dt
    LEFT JOIN {{ ref('seed_sedici-types2coar-types') }} AS seed
      ON LOWER(BTRIM(dt.text_value)) = LOWER(BTRIM(seed.type))
)

SELECT
    text_value,
    resource_type_label_es,
    resource_type_uri,
    matches_seed_mapping,
    CASE
        WHEN resource_type_uri = 'http://purl.org/coar/resource_type/c_2f33' THEN 'book'
        WHEN resource_type_uri = 'http://purl.org/coar/resource_type/c_46ec' THEN 'thesis'
        WHEN LOWER(text_value) LIKE '%libro%' THEN 'book'
        WHEN LOWER(text_value) LIKE '%tesis%' THEN 'thesis'
        ELSE 'other'
    END AS item_type_group
FROM typed
