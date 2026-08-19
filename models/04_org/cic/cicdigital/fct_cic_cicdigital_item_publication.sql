{{ config(materialized='table') }}

WITH item_handle AS (
    SELECT
        handle.resource_id AS item_id,
        handle.handle,
        ROW_NUMBER() OVER (
            PARTITION BY handle.resource_id
            ORDER BY handle.handle_id DESC
        ) AS handle_rank
    FROM {{ source('cicdigital', 'handle') }} AS handle
    WHERE handle.resource_type_id = 2
      AND NULLIF(BTRIM(handle.handle), '') IS NOT NULL
),

final AS (
    SELECT
        item.uuid AS item_id,
        item.item_id AS legacy_item_id,
        item.submitter_id,
        item.in_archive,
        item.withdrawn,
        item.discoverable,
        item.last_modified,
        item.owning_collection AS owning_collection_id,
        collection.collection_title AS owning_collection_title,
        collection.community_id AS owning_community_id,
        collection.community_title AS owning_community_title,
        handle.handle,
        CASE
            WHEN handle.handle IS NOT NULL
                THEN 'https://digital.cic.gba.gob.ar/items/' || item.uuid::text
        END AS item_url,
        title.value_raw AS dc_title_raw,
        title.value AS dc_title,
        subtitle.value_raw AS dcterms_title_subtitle_raw,
        subtitle.value AS dcterms_title_subtitle,
        dc_type.value_raw AS dc_type_raw,
        dc_type.value AS dc_type,
        parent_type.value_raw AS cic_parent_type_raw,
        parent_type.value AS cic_parent_type,
        issued.value_raw AS dcterms_issued_raw,
        issued.value AS dcterms_issued,
        issued.value_precision AS dcterms_issued_precision,
        description.value_raw AS dcterms_description_raw,
        description.value AS dcterms_description
    FROM {{ source('cicdigital', 'item') }} AS item
    LEFT JOIN {{ ref('dim_cic_cicdigital_collection') }} AS collection
        ON collection.collection_id = item.owning_collection
    LEFT JOIN item_handle AS handle
        ON handle.item_id = item.uuid
       AND handle.handle_rank = 1
    LEFT JOIN {{ ref('int_cic_cicdigital_item_dc_title') }} AS title
        ON title.item_id = item.uuid
    LEFT JOIN {{ ref('int_cic_cicdigital_item_dcterms_title_subtitle') }} AS subtitle
        ON subtitle.item_id = item.uuid
    LEFT JOIN {{ ref('int_cic_cicdigital_item_dc_type') }} AS dc_type
        ON dc_type.item_id = item.uuid
    LEFT JOIN {{ ref('int_cic_cicdigital_item_cic_parent_type') }} AS parent_type
        ON parent_type.item_id = item.uuid
    LEFT JOIN {{ ref('int_cic_cicdigital_item_dcterms_issued') }} AS issued
        ON issued.item_id = item.uuid
    LEFT JOIN {{ ref('int_cic_cicdigital_item_dcterms_description') }} AS description
        ON description.item_id = item.uuid
    WHERE item.in_archive IS TRUE
      AND item.withdrawn IS FALSE
)

SELECT * FROM final
