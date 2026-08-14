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
        ownership.collection_title AS owning_collection_title,
        ownership.community_id AS owning_community_id,
        ownership.community_title AS owning_community_title,
        ownership.root_community_id AS owning_root_community_id,
        ownership.root_community_title AS owning_root_community_title,
        ownership.community_path_ids AS owning_community_path_ids,
        ownership.community_path_titles AS owning_community_path_titles,
        item.last_modified,
        dc_type.value AS dc_type,
        sedici_subtype.value AS sedici_subtype,
        title.value_raw AS dc_title_raw,
        title.value AS dc_title,
        date_issued.value_raw AS dc_date_issued_raw,
        date_issued.value AS dc_date_issued,
        date_issued.value_precision AS dc_date_issued_precision,
        EXTRACT(YEAR FROM date_issued.value)::int AS publication_year,
        date_available.value_raw AS dc_date_available_raw,
        date_available.value AS dc_date_available,
        exposure.value_raw AS sedici_date_exposure_raw,
        exposure.value AS sedici_date_exposure,
        exposure.value_precision AS sedici_date_exposure_precision,
        handle.handle,
        CASE
            WHEN handle.handle IS NOT NULL
                THEN 'https://' || item.base_url || '/handle/' || handle.handle
        END AS dc_identifier_uri
    FROM item
    LEFT JOIN {{ ref('dim_unlp_sedicidb_collection') }} AS ownership
        ON ownership.collection_id = item.owning_collection
    LEFT JOIN {{ ref('int_unlp_sedicidb_item_dc_type') }} AS dc_type
        USING (item_id)
    LEFT JOIN {{ ref('int_unlp_sedicidb_item_sedici_subtype') }} AS sedici_subtype
        USING (item_id)
    LEFT JOIN item_handle AS handle
        USING (item_id)
    LEFT JOIN {{ ref('int_unlp_sedicidb_item_dc_title') }} AS title
        USING (item_id)
    LEFT JOIN {{ ref('int_unlp_sedicidb_item_dc_date_issued') }} AS date_issued
        USING (item_id)
    LEFT JOIN {{ ref('int_unlp_sedicidb_item_dc_date_available') }} AS date_available
        USING (item_id)
    LEFT JOIN {{ ref('int_unlp_sedicidb_item_sedici_date_exposure') }} AS exposure
        USING (item_id)
)

SELECT *
FROM final
