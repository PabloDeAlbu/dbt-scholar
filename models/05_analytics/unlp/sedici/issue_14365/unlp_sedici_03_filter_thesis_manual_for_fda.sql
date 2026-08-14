{{ config(materialized='view') }}

SELECT
    koha_id,
    koha_url,
    sedici_handle,
    sedici_url,
    koha_title,
    koha_author,
    koha_date,
    sedici_title,
    sedici_author,
    sedici_date,
    match_source,
    dedup_similarity,
    dedup_rules,
    requires_metadata_review
FROM {{ ref('unlp_sedici_03_base_thesis_koha_sedici_confirmed') }}
WHERE match_source = 'manual'
