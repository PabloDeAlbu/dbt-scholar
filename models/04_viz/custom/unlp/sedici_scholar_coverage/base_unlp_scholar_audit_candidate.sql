{{ config(materialized='view') }}

SELECT
    pub.item_hk,
    pub.item_id,
    pub.institutional_uri,
    pub.date_accessioned,
    pub.publication_date AS date_issued,
    pub.publication_year,
    pub.title,
    pub.publication_type,
    pub.author_count,
    pub.has_doi,
    pub.doi_count,
    pub.doi,
    pub.has_handle,
    pub.handle_count,
    pub.handle,
    pub.owning_collection_title,
    pub.owning_community_title,
    pub.owning_root_community_title,
    pub.matched_by_unique_doi,
    pub.matched_by_unique_original_id
FROM {{ ref('fct_unlp_publication') }} AS pub
WHERE NULLIF(BTRIM(pub.title), '') IS NOT NULL
  AND pub.institutional_uri IS NOT NULL
