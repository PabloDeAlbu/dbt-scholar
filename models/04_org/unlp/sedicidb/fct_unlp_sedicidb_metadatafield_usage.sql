{{ config(
    materialized='table',
    indexes=[
        {'columns': ['metadata_field_id'], 'unique': true},
        {'columns': ['metadatafield_fullname'], 'unique': true},
        {'columns': ['item_count']}
    ],
    post_hook=[
        "analyze {{ this }}"
    ]
) }}

WITH item_universe AS (
    SELECT item_id
    FROM {{ source('sedicidb', 'item') }}
),

repository_stats AS (
    SELECT COUNT(*)::bigint AS total_item_count
    FROM item_universe
),

metadatafield AS (
    SELECT
        field.metadata_field_id,
        field.metadata_schema_id,
        schema_registry.short_id AS metadata_schema,
        field.element,
        NULLIF(BTRIM(field.qualifier), '') AS qualifier,
        CASE
            WHEN NULLIF(BTRIM(field.qualifier), '') IS NOT NULL
                THEN schema_registry.short_id || '.' || field.element || '.' || BTRIM(field.qualifier)
            ELSE schema_registry.short_id || '.' || field.element
        END AS metadatafield_fullname,
        field.scope_note
    FROM {{ source('sedicidb', 'metadatafieldregistry') }} AS field
    INNER JOIN {{ source('sedicidb', 'metadataschemaregistry') }} AS schema_registry
        USING (metadata_schema_id)
),

item_metadata_value AS (
    SELECT
        value.resource_id AS item_id,
        value.metadata_field_id,
        value.metadata_value_id,
        NULLIF(BTRIM(value.text_value), '') AS text_value_clean
    FROM {{ source('sedicidb', 'metadatavalue') }} AS value
    INNER JOIN item_universe AS item
        ON item.item_id = value.resource_id
    WHERE value.resource_type_id = 2
),

item_field_stats AS (
    SELECT
        item_id,
        metadata_field_id,
        COUNT(*)::bigint AS metadata_value_count,
        COUNT(*) FILTER (WHERE text_value_clean IS NOT NULL)::bigint AS nonempty_metadata_value_count,
        COUNT(DISTINCT text_value_clean) FILTER (WHERE text_value_clean IS NOT NULL)::bigint
            AS distinct_nonempty_text_value_count
    FROM item_metadata_value
    GROUP BY item_id, metadata_field_id
),

field_stats AS (
    SELECT
        metadata_field_id,
        COUNT(*)::bigint AS item_count,
        SUM(metadata_value_count)::bigint AS metadata_value_count,
        SUM(nonempty_metadata_value_count)::bigint AS nonempty_metadata_value_count,
        COUNT(*) FILTER (WHERE distinct_nonempty_text_value_count > 1)::bigint AS multivalued_item_count,
        MAX(distinct_nonempty_text_value_count)::bigint AS max_distinct_values_per_item
    FROM item_field_stats
    GROUP BY metadata_field_id
),

field_distinct_values AS (
    SELECT
        metadata_field_id,
        COUNT(DISTINCT text_value_clean)::bigint AS distinct_nonempty_text_value_count
    FROM item_metadata_value
    WHERE text_value_clean IS NOT NULL
    GROUP BY metadata_field_id
),

final AS (
    SELECT
        field.metadata_field_id,
        field.metadata_schema_id,
        field.metadata_schema,
        field.element,
        field.qualifier,
        field.metadatafield_fullname,
        field.scope_note,
        repository.total_item_count,
        COALESCE(stats.item_count, 0) AS item_count,
        COALESCE(stats.metadata_value_count, 0) AS metadata_value_count,
        COALESCE(
            stats.metadata_value_count - stats.nonempty_metadata_value_count,
            0
        ) AS empty_metadata_value_count,
        COALESCE(distinct_values.distinct_nonempty_text_value_count, 0) AS distinct_nonempty_text_value_count,
        COALESCE(stats.multivalued_item_count, 0) AS multivalued_item_count,
        COALESCE(stats.max_distinct_values_per_item, 0) AS max_distinct_values_per_item,
        CASE
            WHEN repository.total_item_count = 0 THEN 0::numeric
            ELSE ROUND(100.0 * COALESCE(stats.item_count, 0) / repository.total_item_count, 4)
        END AS item_coverage_pct
    FROM metadatafield AS field
    CROSS JOIN repository_stats AS repository
    LEFT JOIN field_stats AS stats
        USING (metadata_field_id)
    LEFT JOIN field_distinct_values AS distinct_values
        USING (metadata_field_id)
)

SELECT *
FROM final
