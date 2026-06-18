{{ config(materialized='table') }}

WITH conicet_records AS (
    SELECT
        record_hk,
        record_id
    FROM {{ ref('fct_conicet_oai_record_publication') }}
),

base AS (
    SELECT
        relation.record_hk,
        records.record_id,
        relation.dc_relation,
        relation.relation_type,
        relation.identifier_type,
        relation.identifier_value,
        CASE
            WHEN relation.identifier_value IS NULL THEN 'null'
            WHEN relation.identifier_value = '' THEN 'empty'
            WHEN relation.identifier_value ~* '^10\.[0-9]{4,9}/[^[:space:]]+$' THEN 'valid_doi'
            WHEN relation.identifier_value ~* '^10\.[0-9]{4,9}$' THEN 'truncated_prefix_only'
            WHEN relation.identifier_value ~* '^https?://(dx\.)?doi\.org/' THEN 'doi_url'
            ELSE 'other'
        END AS doi_parse_pattern
    FROM {{ ref('brg_oai_record_relation') }} AS relation
    INNER JOIN conicet_records AS records
        USING (record_hk)
    WHERE relation.relation_type = 'altIdentifier'
      AND relation.identifier_type = 'doi'
),

final AS (
    SELECT *
    FROM base
)

SELECT * FROM final
