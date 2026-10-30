{{ config(materialized='view') }}

-- ¿Qué valor se elige para cada metadatafield?
WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY item_id, metadatafield_fullname
            ORDER BY
                (text_value IS NOT NULL) DESC,
                metadata_value_id DESC
        ) AS value_rank
    FROM {{ ref('int_unlp_sedicidb_item_metadatavalue') }}
),

-- ¿Qué valores tiene cada metadatafield?
aggregated AS (
    SELECT
        item_id,
        metadatafield_fullname,
        ARRAY_AGG(text_value ORDER BY metadata_value_id)
            FILTER (WHERE text_value IS NOT NULL) AS values,
        COUNT(*)::int AS observation_count,
        COUNT(text_value)::int AS value_count,
        COUNT(DISTINCT text_value)::int AS distinct_value_count
    FROM ranked
    GROUP BY
        item_id,
        metadatafield_fullname
)

SELECT
    aggregated.item_id,
    aggregated.metadatafield_fullname,
    ranked.metadata_value_id,
    ranked.text_value_raw AS value_raw,
    ranked.text_value AS value,
    aggregated.values,
    aggregated.observation_count,
    aggregated.value_count,
    aggregated.distinct_value_count,
    aggregated.distinct_value_count > 1 AS has_conflicting_values
FROM aggregated
INNER JOIN ranked
    ON ranked.item_id = aggregated.item_id
    AND ranked.metadatafield_fullname = aggregated.metadatafield_fullname
    AND ranked.value_rank = 1
