WITH solr_candidate AS (
    SELECT
        handle,
        search_resource_id
    FROM {{ ref('seed_unlp_sedici_malvinas_solr_item_candidate') }}
),

excluded AS (
    SELECT
        solr.search_resource_id,
        solr.handle,
        CASE
            WHEN item.item_id IS NULL THEN 'absent_from_dump'
            WHEN item.withdrawn IS TRUE THEN 'withdrawn'
            WHEN item.in_archive IS NOT TRUE THEN 'not_in_archive'
            WHEN item.discoverable IS NOT TRUE THEN 'not_discoverable'
            ELSE 'excluded_from_dedup_fact'
        END::text AS exclusion_reason,
        item.item_id,
        item.in_archive,
        item.withdrawn,
        item.discoverable,
        item.last_modified,
        item.dc_title,
        item.dc_type,
        item.sedici_subtype,
        item.dc_date_issued,
        item.dc_date_available,
        item.sedici_date_exposure,
        item.owning_root_community_title,
        item.owning_community_path_titles,
        item.owning_collection_title
    FROM solr_candidate AS solr
    LEFT JOIN {{ ref('fct_unlp_sedicidb_dedup_publication') }} AS publication
        ON publication.id = solr.handle
    LEFT JOIN {{ ref('fct_unlp_sedicidb_item_publication') }} AS item
        ON item.item_id = solr.search_resource_id
    WHERE publication.id IS NULL
)

SELECT *
FROM excluded
