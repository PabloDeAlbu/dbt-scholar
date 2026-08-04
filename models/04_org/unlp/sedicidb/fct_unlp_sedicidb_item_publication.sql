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

handle_stats AS (
    SELECT
        handle.resource_id AS item_id,
        COUNT(DISTINCT NULLIF(BTRIM(handle.handle), ''))::int AS handle_count,
        MIN(NULLIF(BTRIM(handle.handle), '')) AS handle
    FROM {{ source('sedicidb', 'handle') }} AS handle
    INNER JOIN item
        ON item.item_id = handle.resource_id
    WHERE handle.resource_type_id = 2
    GROUP BY handle.resource_id
),

collection_stats AS (
    SELECT
        relation.item_id,
        COUNT(DISTINCT relation.collection_id)::int AS collections_count
    FROM {{ source('sedicidb', 'collection2item') }} AS relation
    INNER JOIN item
        USING (item_id)
    GROUP BY relation.item_id
),

metadata_stats AS (
    SELECT
        value.resource_id AS item_id,
        COUNT(*)::int AS metadata_value_count,
        COUNT(DISTINCT value.metadata_field_id)::int AS metadatafield_count
    FROM {{ source('sedicidb', 'metadatavalue') }} AS value
    INNER JOIN item
        ON item.item_id = value.resource_id
    WHERE value.resource_type_id = 2
    GROUP BY value.resource_id
),

date_metadatafield AS (
    SELECT
        field.metadata_field_id,
        schema_registry.short_id || '.' || field.element || CASE
            WHEN NULLIF(BTRIM(field.qualifier), '') IS NOT NULL
                THEN '.' || BTRIM(field.qualifier)
            ELSE ''
        END AS metadatafield_fullname
    FROM {{ source('sedicidb', 'metadatafieldregistry') }} AS field
    INNER JOIN {{ source('sedicidb', 'metadataschemaregistry') }} AS schema_registry
        USING (metadata_schema_id)
    WHERE schema_registry.short_id || '.' || field.element || CASE
        WHEN NULLIF(BTRIM(field.qualifier), '') IS NOT NULL
            THEN '.' || BTRIM(field.qualifier)
        ELSE ''
    END IN ('dc.date.issued', 'dc.date.available')
),

date_value AS (
    SELECT
        value.resource_id AS item_id,
        field.metadatafield_fullname,
        NULLIF(BTRIM(value.text_value), '') AS date_raw
    FROM {{ source('sedicidb', 'metadatavalue') }} AS value
    INNER JOIN item
        ON item.item_id = value.resource_id
    INNER JOIN date_metadatafield AS field
        USING (metadata_field_id)
    WHERE value.resource_type_id = 2
),

date_stats AS (
    SELECT
        item_id,
        MIN(date_raw) FILTER (
            WHERE metadatafield_fullname = 'dc.date.issued'
              AND date_raw ~ '^[0-9]{4}[-/](0[1-9]|1[0-2])[-/](0[1-9]|[12][0-9]|3[01])'
        ) AS issued_ymd_raw,
        MIN(date_raw) FILTER (
            WHERE metadatafield_fullname = 'dc.date.issued'
              AND date_raw ~ '^[0-9]{4}[-/](0[1-9]|1[0-2])$'
        ) AS issued_ym_raw,
        MIN(date_raw) FILTER (
            WHERE metadatafield_fullname = 'dc.date.issued'
              AND date_raw ~ '^[0-9]{4}$'
        ) AS issued_year_raw,
        MIN(date_raw) FILTER (
            WHERE metadatafield_fullname = 'dc.date.available'
              AND date_raw ~ '^[0-9]{4}[-/](0[1-9]|1[0-2])[-/](0[1-9]|[12][0-9]|3[01])'
        ) AS available_ymd_raw,
        MIN(date_raw) FILTER (
            WHERE metadatafield_fullname = 'dc.date.available'
              AND date_raw ~ '^[0-9]{4}[-/](0[1-9]|1[0-2])$'
        ) AS available_ym_raw,
        MIN(date_raw) FILTER (
            WHERE metadatafield_fullname = 'dc.date.available'
              AND date_raw ~ '^[0-9]{4}$'
        ) AS available_year_raw
    FROM date_value
    GROUP BY item_id
),

date_prepared AS (
    SELECT
        item_id,
        COALESCE(issued_ymd_raw, issued_ym_raw, issued_year_raw) AS dc_date_issued_raw,
        CASE
            WHEN issued_ymd_raw IS NOT NULL
                THEN TO_DATE(REPLACE(LEFT(issued_ymd_raw, 10), '/', '-'), 'YYYY-MM-DD')
            WHEN issued_ym_raw IS NOT NULL
                THEN TO_DATE(REPLACE(issued_ym_raw, '/', '-') || '-01', 'YYYY-MM-DD')
            WHEN issued_year_raw IS NOT NULL
                THEN TO_DATE(issued_year_raw || '-01-01', 'YYYY-MM-DD')
        END AS dc_date_issued,
        CASE
            WHEN issued_ymd_raw IS NOT NULL THEN 'day'
            WHEN issued_ym_raw IS NOT NULL THEN 'month'
            WHEN issued_year_raw IS NOT NULL THEN 'year'
        END AS dc_date_issued_precision,
        COALESCE(available_ymd_raw, available_ym_raw, available_year_raw) AS dc_date_available_raw,
        CASE
            WHEN available_ymd_raw IS NOT NULL
                THEN TO_DATE(REPLACE(LEFT(available_ymd_raw, 10), '/', '-'), 'YYYY-MM-DD')
            WHEN available_ym_raw IS NOT NULL
                THEN TO_DATE(REPLACE(available_ym_raw, '/', '-') || '-01', 'YYYY-MM-DD')
            WHEN available_year_raw IS NOT NULL
                THEN TO_DATE(available_year_raw || '-01-01', 'YYYY-MM-DD')
        END AS dc_date_available
    FROM date_stats
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
        item.last_modified,
        date.dc_date_issued_raw,
        date.dc_date_issued,
        date.dc_date_issued_precision,
        EXTRACT(YEAR FROM date.dc_date_issued)::int AS publication_year,
        date.dc_date_available_raw,
        date.dc_date_available,
        handle.handle,
        CASE
            WHEN handle.handle IS NOT NULL
                THEN 'https://' || item.base_url || '/handle/' || handle.handle
        END AS dc_identifier_uri,
        COALESCE(handle.handle_count, 0) AS handle_count,
        COALESCE(collection.collections_count, 0) AS collections_count,
        COALESCE(metadata.metadatafield_count, 0) AS metadatafield_count,
        COALESCE(metadata.metadata_value_count, 0) AS metadata_value_count
    FROM item
    LEFT JOIN handle_stats AS handle
        USING (item_id)
    LEFT JOIN collection_stats AS collection
        USING (item_id)
    LEFT JOIN metadata_stats AS metadata
        USING (item_id)
    LEFT JOIN date_prepared AS date
        USING (item_id)
)

SELECT *
FROM final
