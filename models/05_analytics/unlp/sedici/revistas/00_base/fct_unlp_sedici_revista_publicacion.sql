{{ config(
    materialized='table',
    indexes=[
        {'columns': ['item_id'], 'unique': true},
        {'columns': ['sedici_community_id', 'publication_year']},
        {'columns': ['sedici_subtype']}
    ],
    post_hook=["analyze {{ this }}"]
) }}

SELECT
    item.item_id,
    item.handle,
    item.dc_identifier_uri AS item_url,
    item.dc_title AS item_title,
    item.dc_type,
    item.sedici_subtype,
    item.dc_date_issued AS publication_date,
    item.publication_year,
    revista.sedici_community_id,
    revista.sedici_community_title,
    revista.sedici_community_handle,
    revista.is_closed,
    item.owning_root_community_title,
    item.owning_root_child_community_id,
    item.owning_root_child_community_title,
    item.owning_community_path_ids,
    item.owning_community_path_titles,
    item.in_archive,
    item.withdrawn,
    item.discoverable
FROM {{ ref('unlp_sedici_publicacion_base') }} AS item
INNER JOIN {{ ref('dim_unlp_sedici_revista') }} AS revista
    ON revista.sedici_community_id = item.owning_root_child_community_id
WHERE item.handle IS NOT NULL
  AND item.owning_root_community_title = 'Revistas'
