{{ config(materialized='table') }}

WITH base AS (
    SELECT
        record_hk,
        record_id,
        repository_identifier,
        institution_ror,
        dc_identifier_uri,
        dc_relation_doi,
        dc_type,
        dc_type_count,
        has_multiple_dc_type,
        has_doi,
        has_multiple_doi
    FROM {{ ref('fct_conicet_oai_record_publication') }}
),

classified AS (
    SELECT
        record_hk,
        record_id,
        repository_identifier,
        institution_ror,
        dc_identifier_uri,
        CASE
            WHEN dc_identifier_uri IS NULL THEN 'null'
            WHEN dc_identifier_uri ~ '^http://hdl\\.handle\\.net/11336/' THEN 'http_handle_11336'
            WHEN dc_identifier_uri ~ '^https://hdl\\.handle\\.net/11336/' THEN 'https_handle_11336'
            WHEN dc_identifier_uri ~ '11336' THEN 'contains_11336_other'
            ELSE 'other'
        END AS dc_identifier_uri_pattern,
        COALESCE(
            dc_identifier_uri ~ '^https?://hdl\\.handle\\.net/11336/',
            false
        ) AS dc_identifier_uri_is_valid,
        dc_relation_doi,
        CASE
            WHEN dc_relation_doi IS NULL THEN 'null'
            WHEN dc_relation_doi ~* '^10\\.[0-9]{4,9}/\\S+$' THEN 'bare_doi'
            WHEN dc_relation_doi ~* '^https?://(dx\\.)?doi\\.org/' THEN 'doi_url'
            WHEN dc_relation_doi ~* 'doi' THEN 'contains_doi_text'
            ELSE 'other'
        END AS dc_relation_doi_pattern,
        COALESCE(
            dc_relation_doi IS NULL OR dc_relation_doi ~* '^10\\.[0-9]{4,9}/\\S+$',
            false
        ) AS dc_relation_doi_is_valid,
        dc_type,
        dc_type_count,
        has_multiple_dc_type,
        has_doi,
        has_multiple_doi
    FROM base
)

SELECT * FROM classified
