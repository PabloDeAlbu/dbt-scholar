{{ config(materialized='table') }}

WITH item_universe AS (
    SELECT uuid AS item_id
    FROM {{ source('cicdigital', 'item') }}
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
        schema_registry.short_id || '.' || field.element || CASE
            WHEN NULLIF(BTRIM(field.qualifier), '') IS NOT NULL
                THEN '.' || BTRIM(field.qualifier)
            ELSE ''
        END AS metadatafield_fullname,
        field.scope_note
    FROM {{ source('cicdigital', 'metadatafieldregistry') }} AS field
    INNER JOIN {{ source('cicdigital', 'metadataschemaregistry') }} AS schema_registry
        USING (metadata_schema_id)
),

item_metadata_value AS (
    SELECT
        value.dspace_object_id AS item_id,
        value.metadata_field_id,
        NULLIF(BTRIM(value.text_value), '') AS value,
        NULLIF(LOWER(BTRIM(value.text_lang)), '') AS text_lang,
        NULLIF(BTRIM(value.authority), '') AS authority
    FROM {{ source('cicdigital', 'metadatavalue') }} AS value
    INNER JOIN item_universe AS item
        ON item.item_id = value.dspace_object_id
),

item_field_stats AS (
    SELECT
        item_id,
        metadata_field_id,
        COUNT(*)::bigint AS metadata_value_count,
        COUNT(*) FILTER (WHERE value IS NOT NULL)::bigint AS nonempty_metadata_value_count,
        COUNT(DISTINCT value) FILTER (WHERE value IS NOT NULL)::bigint AS distinct_value_count
    FROM item_metadata_value
    GROUP BY item_id, metadata_field_id
),

field_stats AS (
    SELECT
        metadata_field_id,
        COUNT(*)::bigint AS item_count,
        SUM(metadata_value_count)::bigint AS metadata_value_count,
        SUM(nonempty_metadata_value_count)::bigint AS nonempty_metadata_value_count,
        COUNT(*) FILTER (WHERE distinct_value_count > 1)::bigint AS multivalued_item_count,
        MAX(distinct_value_count)::bigint AS max_distinct_values_per_item
    FROM item_field_stats
    GROUP BY metadata_field_id
),

field_value_stats AS (
    SELECT
        metadata_field_id,
        COUNT(DISTINCT value)::bigint AS distinct_nonempty_value_count,
        COUNT(*) FILTER (WHERE text_lang IS NOT NULL)::bigint AS text_lang_value_count,
        COUNT(DISTINCT text_lang)::bigint AS distinct_text_lang_count,
        COUNT(*) FILTER (WHERE authority IS NOT NULL)::bigint AS authority_value_count,
        COUNT(DISTINCT authority)::bigint AS distinct_authority_count
    FROM item_metadata_value
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
        COALESCE(stats.metadata_value_count - stats.nonempty_metadata_value_count, 0)
            AS empty_metadata_value_count,
        COALESCE(value_stats.distinct_nonempty_value_count, 0) AS distinct_nonempty_value_count,
        COALESCE(stats.multivalued_item_count, 0) AS multivalued_item_count,
        COALESCE(stats.max_distinct_values_per_item, 0) AS max_distinct_values_per_item,
        CASE
            WHEN repository.total_item_count = 0 THEN 0::numeric
            ELSE ROUND(100.0 * COALESCE(stats.item_count, 0) / repository.total_item_count, 4)
        END AS item_coverage_pct,
        COALESCE(value_stats.text_lang_value_count, 0) AS text_lang_value_count,
        COALESCE(stats.metadata_value_count - value_stats.text_lang_value_count, 0)
            AS missing_text_lang_value_count,
        CASE
            WHEN COALESCE(stats.metadata_value_count, 0) = 0 THEN 0::numeric
            ELSE ROUND(
                100.0 * COALESCE(value_stats.text_lang_value_count, 0) / stats.metadata_value_count,
                4
            )
        END AS text_lang_coverage_pct,
        COALESCE(value_stats.distinct_text_lang_count, 0) AS distinct_text_lang_count,
        COALESCE(value_stats.authority_value_count, 0) AS authority_value_count,
        COALESCE(stats.metadata_value_count - value_stats.authority_value_count, 0)
            AS missing_authority_value_count,
        CASE
            WHEN COALESCE(stats.metadata_value_count, 0) = 0 THEN 0::numeric
            ELSE ROUND(
                100.0 * COALESCE(value_stats.authority_value_count, 0) / stats.metadata_value_count,
                4
            )
        END AS authority_coverage_pct,
        COALESCE(value_stats.distinct_authority_count, 0) AS distinct_authority_count
    FROM metadatafield AS field
    CROSS JOIN repository_stats AS repository
    LEFT JOIN field_stats AS stats
        USING (metadata_field_id)
    LEFT JOIN field_value_stats AS value_stats
        USING (metadata_field_id)
)

SELECT * FROM final
