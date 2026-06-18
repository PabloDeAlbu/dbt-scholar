{{ config(materialized='table') }}

WITH base AS (
    SELECT *
    FROM {{ ref('fct_conicet_oai_record_pid_profile') }}
),

identifier_summary AS (
    SELECT
        'dc_identifier_uri'::text AS pid_dimension,
        dc_identifier_uri_pattern AS pid_pattern,
        COUNT(*) AS record_count,
        COUNT(*) FILTER (WHERE dc_identifier_uri_is_valid) AS valid_record_count,
        COUNT(*) FILTER (WHERE NOT dc_identifier_uri_is_valid) AS invalid_record_count
    FROM base
    GROUP BY dc_identifier_uri_pattern
),

doi_summary AS (
    SELECT
        'dc_relation_doi'::text AS pid_dimension,
        dc_relation_doi_pattern AS pid_pattern,
        COUNT(*) AS record_count,
        COUNT(*) FILTER (WHERE dc_relation_doi_is_valid) AS valid_record_count,
        COUNT(*) FILTER (WHERE NOT dc_relation_doi_is_valid) AS invalid_record_count
    FROM base
    GROUP BY dc_relation_doi_pattern
),

unioned AS (
    SELECT * FROM identifier_summary
    UNION ALL
    SELECT * FROM doi_summary
)

SELECT * FROM unioned
