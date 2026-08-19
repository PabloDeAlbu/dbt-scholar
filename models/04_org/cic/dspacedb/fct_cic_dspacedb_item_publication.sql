{{ config(materialized='table') }}

SELECT
    item.*,
    REGEXP_REPLACE(item.base_url, '/+$', '') || '/items/' || item.item_uuid || '/' AS item_url,
    collection.collection_url AS owning_collection_url,
    collection.collection_title AS owning_collection_title,
    collection.community_uuid AS owning_community_id,
    collection.community_title AS owning_community_title,
    collection.community_url AS owning_community_url,
    identifier_uri.value_raw AS dc_identifier_uri_raw,
    identifier_uri.value AS dc_identifier_uri,
    available.value_raw AS dc_date_available_raw,
    available.value AS dc_date_available,
    available.value_precision AS dc_date_available_precision,
    issued.value_raw AS dcterms_issued_raw,
    issued.value AS dcterms_issued,
    issued.value_precision AS dcterms_issued_precision,
    title.value_raw AS dc_title_raw,
    title.value AS dc_title,
    subtitle.value_raw AS dcterms_title_subtitle_raw,
    subtitle.value AS dcterms_title_subtitle,
    parent_type.value_raw AS cic_parent_type_raw,
    parent_type.value AS cic_parent_type,
    dc_type.value_raw AS dc_type_raw,
    dc_type.value AS dc_type,
    description.value_raw AS dcterms_description_raw,
    description.value AS dcterms_description
FROM {{ ref('int_cic_dspacedb_item') }} AS item
LEFT JOIN {{ ref('dim_dspacedb_collection') }} AS collection
    ON collection.collection_uuid = item.owning_collection
   AND collection.base_url = item.base_url
   AND collection.source_label = item.source_label
   AND collection.institution_ror = item.institution_ror
LEFT JOIN {{ ref('int_cic_dspacedb_item_dc_date_available') }} AS available
    USING (item_uuid)
LEFT JOIN {{ ref('int_cic_dspacedb_item_dc_identifier_uri') }} AS identifier_uri
    USING (item_uuid)
LEFT JOIN {{ ref('int_cic_dspacedb_item_dcterms_issued') }} AS issued
    USING (item_uuid)
LEFT JOIN {{ ref('int_cic_dspacedb_item_dc_title') }} AS title
    USING (item_uuid)
LEFT JOIN {{ ref('int_cic_dspacedb_item_dcterms_title_subtitle') }} AS subtitle
    USING (item_uuid)
LEFT JOIN {{ ref('int_cic_dspacedb_item_cic_parent_type') }} AS parent_type
    USING (item_uuid)
LEFT JOIN {{ ref('int_cic_dspacedb_item_dc_type') }} AS dc_type
    USING (item_uuid)
LEFT JOIN {{ ref('int_cic_dspacedb_item_dcterms_description') }} AS description
    USING (item_uuid)
