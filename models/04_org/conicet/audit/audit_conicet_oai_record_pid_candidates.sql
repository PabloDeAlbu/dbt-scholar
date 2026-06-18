{{ config(materialized = 'table') }}

WITH conicet_record AS (
    SELECT
        record_hk,
        record_id
    FROM {{ ref('fct_conicet_oai_record_publication') }}
),

identifier_candidate AS (
    SELECT
        record.record_hk,
        record.record_id,
        'dc_identifier'::text AS source_field,
        identifier.record_identifier_hk AS source_row_hk,
        NULL::text AS relation_type,
        NULL::text AS identifier_type,
        NULL::text AS identifier_value,
        'not_applicable'::text AS relation_parse_status,
        identifier.dc_identifier AS raw_value
    FROM conicet_record AS record
    INNER JOIN {{ ref('brg_oai_record_identifier') }} AS identifier
        USING (record_hk)
),

relation_candidate AS (
    SELECT
        record.record_hk,
        record.record_id,
        'dc_relation'::text AS source_field,
        relation.record_relation_hk AS source_row_hk,
        CASE
            WHEN relation.dc_relation ~ '^info:eu-repo/semantics/[^/]+/[^/]+/.+'
            THEN REGEXP_REPLACE(
                relation.dc_relation,
                '^info:eu-repo/semantics/([^/]+)/([^/]+)/(.*)$',
                '\1'
            )
        END AS relation_type,
        CASE
            WHEN relation.dc_relation ~ '^info:eu-repo/semantics/[^/]+/[^/]+/.+'
            THEN REGEXP_REPLACE(
                relation.dc_relation,
                '^info:eu-repo/semantics/([^/]+)/([^/]+)/(.*)$',
                '\2'
            )
        END AS identifier_type,
        CASE
            WHEN relation.dc_relation ~ '^info:eu-repo/semantics/[^/]+/[^/]+/.+'
            THEN REGEXP_REPLACE(
                relation.dc_relation,
                '^info:eu-repo/semantics/([^/]+)/([^/]+)/(.*)$',
                '\3'
            )
        END AS identifier_value,
        CASE
            WHEN relation.dc_relation ~ '^info:eu-repo/semantics/[^/]+/[^/]+/.+'
            THEN 'eu_repo_semantic'
            WHEN relation.dc_relation ~* '^https?://'
            THEN 'raw_url'
            WHEN relation.dc_relation ~ '^[^/]+/[^/]+$'
            THEN 'slash_text'
            ELSE 'free_text'
        END AS relation_parse_status,
        relation.dc_relation AS raw_value
    FROM conicet_record AS record
    INNER JOIN {{ ref('brg_oai_record_relation') }} AS relation
        USING (record_hk)
),

unioned AS (
    SELECT * FROM identifier_candidate
    UNION ALL
    SELECT * FROM relation_candidate
),

classified AS (
    SELECT
        record_hk,
        record_id,
        source_field,
        source_row_hk,
        relation_type,
        identifier_type,
        identifier_value,
        relation_parse_status,
        raw_value,
        (
            source_field = 'dc_relation'
            AND relation_type = 'altIdentifier'
            AND identifier_type = 'doi'
            AND identifier_value IS NOT NULL
            AND TRIM(identifier_value) <> ''
        ) AS enters_current_doi_rule,
        CASE
            WHEN source_field = 'dc_relation'
                 AND relation_parse_status = 'eu_repo_semantic'
                 AND LOWER(COALESCE(identifier_type, '')) = 'doi'
            THEN 'doi'
            WHEN source_field = 'dc_relation'
                 AND relation_parse_status = 'eu_repo_semantic'
                 AND LOWER(COALESCE(identifier_type, '')) = 'url'
            THEN 'url'
            WHEN source_field = 'dc_relation'
                 AND relation_parse_status = 'eu_repo_semantic'
                 AND LOWER(COALESCE(identifier_type, '')) = 'issn'
            THEN 'issn'
            WHEN source_field = 'dc_relation'
                 AND relation_parse_status = 'eu_repo_semantic'
                 AND LOWER(COALESCE(identifier_type, '')) = 'isbn'
            THEN 'isbn'
            WHEN source_field = 'dc_relation'
                 AND relation_parse_status = 'eu_repo_semantic'
                 AND LOWER(COALESCE(identifier_type, '')) = 'arxiv'
            THEN 'arxiv'
            WHEN source_field = 'dc_relation'
                 AND relation_parse_status = 'eu_repo_semantic'
                 AND LOWER(COALESCE(identifier_type, '')) = 'pmid'
            THEN 'pmid'
            WHEN source_field = 'dc_relation'
                 AND relation_parse_status = 'eu_repo_semantic'
                 AND LOWER(COALESCE(identifier_type, '')) = 'ark'
            THEN 'ark'
            WHEN source_field = 'dc_relation'
                 AND relation_parse_status = 'eu_repo_semantic'
                 AND LOWER(COALESCE(identifier_type, '')) IN ('hdl', 'handle')
            THEN 'handle'
            WHEN source_field = 'dc_relation'
                 AND relation_parse_status = 'eu_repo_semantic'
                 AND LOWER(COALESCE(identifier_type, '')) = 'urn'
            THEN 'urn'
            WHEN source_field = 'dc_relation'
                 AND relation_parse_status = 'eu_repo_semantic'
                 AND LOWER(COALESCE(identifier_type, '')) = 'wos'
            THEN 'wos'
            WHEN source_field = 'dc_relation'
                 AND relation_parse_status = 'eu_repo_semantic'
                 AND LOWER(COALESCE(identifier_type, '')) = 'purl'
            THEN 'purl'
            WHEN raw_value IS NULL OR TRIM(raw_value) = '' THEN 'empty'
            WHEN raw_value ~* '^10\.[0-9]{4,9}/\S+$' THEN 'doi'
            WHEN raw_value ~* '^https?://(dx\.)?doi\.org/10\.[0-9]{4,9}/\S+$' THEN 'doi_url'
            WHEN raw_value ~* '^doi:\s*10\.[0-9]{4,9}/\S+$' THEN 'doi_prefixed'
            WHEN raw_value ~* '^https?://hdl\.handle\.net/' THEN 'handle_url'
            WHEN raw_value ~* '^hdl:\S+$' THEN 'handle_compact'
            WHEN raw_value ~* '^https?://orcid\.org/[0-9X-]{19}$' THEN 'orcid_url'
            WHEN raw_value ~* '^[0-9]{4}-[0-9]{4}-[0-9]{3}[0-9X]$' THEN 'orcid'
            WHEN raw_value ~* '^(arxiv:)?[0-9]{4}\.[0-9]{4,5}(v[0-9]+)?$' THEN 'arxiv'
            WHEN raw_value ~* '^(97(8|9))?[0-9-]{10,17}[0-9X]$' THEN 'isbn'
            WHEN raw_value ~* '^[0-9]{4}-[0-9]{3}[0-9X]$' THEN 'issn'
            WHEN raw_value ~* '^https?://' THEN 'url_other'
            ELSE 'other'
        END AS pid_candidate_type,
        CASE
            WHEN source_field = 'dc_relation'
                 AND relation_type = 'altIdentifier'
                 AND identifier_type = 'doi'
            THEN 'current_doi_relation'
            WHEN source_field = 'dc_identifier'
                 AND raw_value LIKE 'http://hdl.handle.net/11336/%'
            THEN 'current_repository_identifier'
            ELSE 'outside_current_conicet_pid_rules'
        END AS current_rule_status
    FROM unioned
),

final AS (
    SELECT
        record_hk,
        record_id,
        source_field,
        source_row_hk,
        relation_type,
        identifier_type,
        identifier_value,
        relation_parse_status,
        raw_value,
        enters_current_doi_rule,
        pid_candidate_type,
        current_rule_status
    FROM classified
)

SELECT * FROM final
