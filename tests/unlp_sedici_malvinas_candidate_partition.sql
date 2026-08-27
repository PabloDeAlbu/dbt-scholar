WITH counts AS (
    SELECT
        (SELECT COUNT(*) FROM {{ ref('seed_unlp_sedici_malvinas_solr_item_candidate') }}) AS seed_count,
        (SELECT COUNT(*) FROM {{ ref('unlp_sedici_malvinas_publication_candidate') }}) AS included_count,
        (SELECT COUNT(*) FROM {{ ref('unlp_sedici_malvinas_item_candidate_excluded') }}) AS excluded_count
)

SELECT *
FROM counts
WHERE seed_count <> included_count + excluded_count
