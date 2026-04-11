{{ config(materialized='view') }}

WITH base AS (
    SELECT
        pid_dimension,
        pid_pattern,
        record_count,
        valid_record_count,
        invalid_record_count
    FROM {{ ref('fct_conicet_oai_record_pid_coverage') }}
),

long_format AS (
    SELECT
        pid_dimension,
        pid_pattern,
        quality_status,
        quality_record_count AS record_count
    FROM base
    CROSS JOIN LATERAL (
        VALUES
            (
                CASE
                    WHEN pid_dimension = 'dc_relation_doi'
                     AND pid_pattern = 'null'
                    THEN 'missing'
                    ELSE 'valid'
                END,
                valid_record_count
            ),
            ('invalid', invalid_record_count)
    ) AS status(quality_status, quality_record_count)
    WHERE quality_record_count > 0
),

enriched AS (
    SELECT
        pid_dimension,
        CASE
            WHEN pid_dimension = 'dc_identifier_uri' THEN 'Identifier URI'
            WHEN pid_dimension = 'dc_relation_doi' THEN 'DOI'
            ELSE pid_dimension
        END AS pid_label,
        pid_pattern,
        CASE
            WHEN pid_dimension = 'dc_identifier_uri' AND pid_pattern = 'http_handle_11336' THEN 10
            WHEN pid_dimension = 'dc_identifier_uri' AND pid_pattern = 'https_handle_11336' THEN 20
            WHEN pid_dimension = 'dc_identifier_uri' AND pid_pattern = 'contains_11336_other' THEN 30
            WHEN pid_dimension = 'dc_identifier_uri' AND pid_pattern = 'other' THEN 40
            WHEN pid_dimension = 'dc_identifier_uri' AND pid_pattern = 'null' THEN 50
            WHEN pid_dimension = 'dc_relation_doi' AND pid_pattern = 'bare_doi' THEN 10
            WHEN pid_dimension = 'dc_relation_doi' AND pid_pattern = 'doi_url' THEN 20
            WHEN pid_dimension = 'dc_relation_doi' AND pid_pattern = 'contains_doi_text' THEN 30
            WHEN pid_dimension = 'dc_relation_doi' AND pid_pattern = 'other' THEN 40
            WHEN pid_dimension = 'dc_relation_doi' AND pid_pattern = 'null' THEN 50
            ELSE 999
        END AS pattern_order,
        quality_status,
        record_count
    FROM long_format
)

SELECT * FROM enriched
