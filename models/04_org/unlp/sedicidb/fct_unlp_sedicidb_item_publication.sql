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

dc_title_raw_agg AS (
    SELECT
        item_id,
        STRING_AGG(
            dc_title_raw,
            '|'
            ORDER BY place NULLS LAST, metadata_value_id
        ) FILTER (WHERE dc_title_raw IS NOT NULL) AS dc_title_raw
    FROM {{ ref('int_unlp_sedicidb_item_dc_title') }}
    GROUP BY item_id
),

dc_title_distinct AS (
    SELECT
        item_id,
        LOWER(dc_title) AS dc_title_key,
        MIN(dc_title) AS dc_title,
        MIN(place) AS first_place,
        MIN(metadata_value_id) AS first_metadata_value_id
    FROM {{ ref('int_unlp_sedicidb_item_dc_title') }}
    WHERE dc_title IS NOT NULL
    GROUP BY item_id, LOWER(dc_title)
),

dc_title_agg AS (
    SELECT
        item_id,
        STRING_AGG(
            dc_title,
            '|'
            ORDER BY first_place NULLS LAST, first_metadata_value_id, dc_title
        ) AS dc_title
    FROM dc_title_distinct
    GROUP BY item_id
),

author_value AS (
    SELECT
        item_id,
        metadata_value_id,
        'sedici_creator_person'::text AS author_field,
        sedici_creator_person_raw AS author_raw,
        sedici_creator_person AS author,
        place
    FROM {{ ref('int_unlp_sedicidb_item_sedici_creator_person') }}

    UNION ALL

    SELECT
        item_id,
        metadata_value_id,
        'sedici_creator_corporate'::text AS author_field,
        sedici_creator_corporate_raw AS author_raw,
        sedici_creator_corporate AS author,
        place
    FROM {{ ref('int_unlp_sedicidb_item_sedici_creator_corporate') }}

    UNION ALL

    SELECT
        item_id,
        metadata_value_id,
        'sedici_contributor_compiler'::text AS author_field,
        sedici_contributor_compiler_raw AS author_raw,
        sedici_contributor_compiler AS author,
        place
    FROM {{ ref('int_unlp_sedicidb_item_sedici_contributor_compiler') }}
),

author_raw_agg AS (
    SELECT
        item_id,
        author_field,
        STRING_AGG(
            author_raw,
            '|'
            ORDER BY place NULLS LAST, metadata_value_id
        ) FILTER (WHERE author_raw IS NOT NULL) AS author_raw
    FROM author_value
    GROUP BY item_id, author_field
),

author_distinct AS (
    SELECT
        item_id,
        author_field,
        LOWER(author) AS author_key,
        MIN(author) AS author,
        MIN(place) AS first_place,
        MIN(metadata_value_id) AS first_metadata_value_id
    FROM author_value
    WHERE author IS NOT NULL
    GROUP BY item_id, author_field, LOWER(author)
),

author_agg AS (
    SELECT
        item_id,
        author_field,
        STRING_AGG(
            author,
            '|'
            ORDER BY first_place NULLS LAST, first_metadata_value_id, author
        ) AS author
    FROM author_distinct
    GROUP BY item_id, author_field
),

author_metadata AS (
    SELECT
        item_id,
        MAX(raw.author_raw) FILTER (
            WHERE raw.author_field = 'sedici_creator_person'
        ) AS sedici_creator_person_raw,
        MAX(author.author) FILTER (
            WHERE author.author_field = 'sedici_creator_person'
        ) AS sedici_creator_person,
        MAX(raw.author_raw) FILTER (
            WHERE raw.author_field = 'sedici_creator_corporate'
        ) AS sedici_creator_corporate_raw,
        MAX(author.author) FILTER (
            WHERE author.author_field = 'sedici_creator_corporate'
        ) AS sedici_creator_corporate,
        MAX(raw.author_raw) FILTER (
            WHERE raw.author_field = 'sedici_contributor_compiler'
        ) AS sedici_contributor_compiler_raw,
        MAX(author.author) FILTER (
            WHERE author.author_field = 'sedici_contributor_compiler'
        ) AS sedici_contributor_compiler
    FROM author_raw_agg AS raw
    FULL OUTER JOIN author_agg AS author
        USING (item_id, author_field)
    GROUP BY item_id
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
        title_raw.dc_title_raw,
        title.dc_title,
        author.sedici_creator_person_raw,
        author.sedici_creator_person,
        author.sedici_creator_corporate_raw,
        author.sedici_creator_corporate,
        author.sedici_contributor_compiler_raw,
        author.sedici_contributor_compiler,
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
    LEFT JOIN dc_title_raw_agg AS title_raw
        USING (item_id)
    LEFT JOIN dc_title_agg AS title
        USING (item_id)
    LEFT JOIN author_metadata AS author
        USING (item_id)
    LEFT JOIN date_prepared AS date
        USING (item_id)
)

SELECT *
FROM final
