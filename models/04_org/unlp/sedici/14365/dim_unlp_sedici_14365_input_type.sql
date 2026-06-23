{{ config(
    materialized='table',
    indexes=[
      {'columns': ['type_raw'], 'unique': true},
      {'columns': ['dedup_type']}
    ],
    post_hook=["analyze {{ this }}"]
) }}

WITH sedici_raw_type AS (
    SELECT DISTINCT
        NULLIF({{ clean_text('type_raw') }}, '')::text AS type_raw
    FROM {{ ref('fct_unlp_sedici_dedup_publication') }}
    WHERE NULLIF({{ clean_text('type_raw') }}, '') IS NOT NULL
),

typed AS (
    SELECT
        s.type_raw,
        seed.coar_uri AS resource_type_uri,
        CASE
            WHEN seed.coar_uri = 'http://purl.org/coar/resource_type/c_2f33' THEN 'LIBRO'
            WHEN seed.coar_uri = 'http://purl.org/coar/resource_type/c_46ec' THEN 'TESIS'
            WHEN LOWER(s.type_raw) LIKE '%libro%' THEN 'LIBRO'
            WHEN LOWER(s.type_raw) LIKE '%tesis%' THEN 'TESIS'
        END AS dedup_type
    FROM sedici_raw_type AS s
    LEFT JOIN {{ ref('seed_sedici-types2coar-types') }} AS seed
      ON LOWER(BTRIM(seed.type)) = LOWER(BTRIM(s.type_raw))
)

SELECT
    type_raw,
    resource_type_uri,
    CASE
        WHEN dedup_type = 'LIBRO' THEN 'book'
        WHEN dedup_type = 'TESIS' THEN 'thesis'
    END AS item_type_group,
    dedup_type,
    dedup_type IS NOT NULL AS is_dedup_supported
FROM typed
