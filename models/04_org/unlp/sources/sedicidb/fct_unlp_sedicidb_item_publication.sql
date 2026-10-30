{{ config(
    materialized='table',
    indexes=[
        {'columns': ['item_id'], 'unique': true},
        {'columns': ['in_archive', 'withdrawn', 'discoverable']},
        {'columns': ['owning_collection']}
    ],
    post_hook=[
        "analyze {{ this }}"
    ]
) }}

-- ¿Qué ítems se representan?
WITH item AS (
    SELECT
        item_id,
        submitter_id,
        in_archive,
        withdrawn,
        discoverable,
        owning_collection,
        last_modified,
        'sedici'::text AS source_label,
        'https://ror.org/01tjs6929'::text AS institution_ror,
        'sedici.unlp.edu.ar'::text AS base_url
    FROM {{ source('sedicidb', 'item') }}
),

-- ¿Qué handle tiene cada ítem?
item_handle AS (
    SELECT
        handle.resource_id AS item_id,
        MIN(NULLIF(BTRIM(handle.handle), '')) AS handle
    FROM {{ source('sedicidb', 'handle') }} AS handle
    INNER JOIN item
        ON item.item_id = handle.resource_id
    WHERE handle.resource_type_id = 2
    GROUP BY handle.resource_id
),

-- ¿Qué título, tipo y subtipo usa cada ítem?
item_metadatafield AS (
    SELECT *
    FROM {{ ref('int_unlp_sedicidb_item_metadatafield') }}
    WHERE metadatafield_fullname IN (
        'dc.title',
        'dc.type',
        'sedici.subtype'
    )
),

-- ¿Qué fechas usa cada ítem?
item_date_metadatafield AS (
    SELECT *
    FROM {{ ref('int_unlp_sedicidb_item_date_metadatafield') }}
),

-- ¿Cómo se representa cada publicación?
final AS (
    SELECT
        item.item_id,
        item.source_label,
        item.institution_ror,
        item.base_url,
        item.submitter_id,
        item.in_archive,
        item.withdrawn,
        item.discoverable,
        item.owning_collection,
        collection.collection_title AS owning_collection_title,
        collection.community_id AS owning_community_id,
        collection.community_title AS owning_community_title,
        collection.root_community_id AS owning_root_community_id,
        collection.root_community_title AS owning_root_community_title,
        collection.root_child_community_id AS owning_root_child_community_id,
        collection.root_child_community_title AS owning_root_child_community_title,
        collection.community_path_ids AS owning_community_path_ids,
        collection.community_path_titles AS owning_community_path_titles,
        item.last_modified,
        NULLIF(REPLACE(dc_type.value, '|', ' '), '') AS dc_type,
        NULLIF(REPLACE(sedici_subtype.value, '|', ' '), '') AS sedici_subtype,
        title.value_raw AS dc_title_raw,
        NULLIF(
            TRIM(REGEXP_REPLACE(REPLACE(title.value, '|', ' '), '\s+', ' ', 'g')),
            ''
        ) AS dc_title,
        date_issued.value_raw AS dc_date_issued_raw,
        date_issued.value AS dc_date_issued,
        date_issued.date_precision AS dc_date_issued_precision,
        EXTRACT(YEAR FROM date_issued.value)::int AS publication_year,
        date_accessioned.value_raw AS dc_date_accessioned_raw,
        date_accessioned.value AS dc_date_accessioned,
        date_available.value_raw AS dc_date_available_raw,
        date_available.value AS dc_date_available,
        exposure.value_raw AS sedici_date_exposure_raw,
        exposure.value AS sedici_date_exposure,
        exposure.date_precision AS sedici_date_exposure_precision,
        handle.handle,
        CASE
            WHEN handle.handle IS NOT NULL
                THEN 'https://' || item.base_url || '/handle/' || handle.handle
        END AS dc_identifier_uri
    FROM item
    -- ¿En qué colección y comunidades está publicado el ítem?
    LEFT JOIN {{ ref('dim_unlp_sedicidb_collection') }} AS collection
        ON collection.collection_id = item.owning_collection
    LEFT JOIN item_metadatafield AS dc_type
        ON dc_type.item_id = item.item_id
        AND dc_type.metadatafield_fullname = 'dc.type'
    LEFT JOIN item_metadatafield AS sedici_subtype
        ON sedici_subtype.item_id = item.item_id
        AND sedici_subtype.metadatafield_fullname = 'sedici.subtype'
    LEFT JOIN item_handle AS handle
        ON handle.item_id = item.item_id
    LEFT JOIN item_metadatafield AS title
        ON title.item_id = item.item_id
        AND title.metadatafield_fullname = 'dc.title'
    LEFT JOIN item_date_metadatafield AS date_issued
        ON date_issued.item_id = item.item_id
        AND date_issued.metadatafield_fullname = 'dc.date.issued'
    LEFT JOIN item_date_metadatafield AS date_accessioned
        ON date_accessioned.item_id = item.item_id
        AND date_accessioned.metadatafield_fullname = 'dc.date.accessioned'
    LEFT JOIN item_date_metadatafield AS date_available
        ON date_available.item_id = item.item_id
        AND date_available.metadatafield_fullname = 'dc.date.available'
    LEFT JOIN item_date_metadatafield AS exposure
        ON exposure.item_id = item.item_id
        AND exposure.metadatafield_fullname = 'sedici.date.exposure'
)

SELECT *
FROM final
