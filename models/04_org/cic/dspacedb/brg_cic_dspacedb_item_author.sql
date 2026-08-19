{{ config(materialized='view') }}

WITH author AS (
    SELECT item_uuid, metadata_value_id, 'author'::text AS author_role,
        value_raw AS author_name_raw, value AS author_name,
        authority, confidence, place
    FROM {{ ref('int_cic_dspacedb_item_dcterms_creator_author') }}

    UNION ALL

    SELECT item_uuid, metadata_value_id, 'corporate'::text AS author_role,
        value_raw AS author_name_raw, value AS author_name,
        authority, confidence, place
    FROM {{ ref('int_cic_dspacedb_item_dcterms_creator_corporate') }}

    UNION ALL

    SELECT item_uuid, metadata_value_id, 'editor'::text AS author_role,
        value_raw AS author_name_raw, value AS author_name,
        authority, confidence, place
    FROM {{ ref('int_cic_dspacedb_item_dcterms_creator_editor') }}

    UNION ALL

    SELECT item_uuid, metadata_value_id, 'compilator'::text AS author_role,
        value_raw AS author_name_raw, value AS author_name,
        authority, confidence, place
    FROM {{ ref('int_cic_dspacedb_item_dcterms_creator_compilator') }}
)

SELECT * FROM author
