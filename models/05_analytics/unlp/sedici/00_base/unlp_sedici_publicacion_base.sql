{{ config(materialized='view') }}

SELECT
    item_id,
    source_label,
    institution_ror,
    base_url,
    submitter_id,
    in_archive,
    withdrawn,
    discoverable,
    owning_collection,
    owning_collection_title,
    owning_community_id,
    owning_community_title,
    owning_root_community_id,
    owning_root_community_title,
    owning_root_child_community_id,
    owning_root_child_community_title,
    owning_community_path_ids,
    owning_community_path_titles,
    last_modified,
    dc_type,
    sedici_subtype,
    dc_title_raw,
    dc_title,
    dc_date_issued_raw,
    dc_date_issued,
    dc_date_issued_precision,
    publication_year,
    dc_date_accessioned_raw,
    dc_date_accessioned,
    dc_date_available_raw,
    dc_date_available,
    sedici_date_exposure_raw,
    sedici_date_exposure,
    sedici_date_exposure_precision,
    handle,
    dc_identifier_uri
FROM {{ ref('fct_unlp_sedicidb_item_publication') }}
WHERE in_archive IS TRUE
  AND withdrawn IS FALSE
  AND discoverable IS TRUE
