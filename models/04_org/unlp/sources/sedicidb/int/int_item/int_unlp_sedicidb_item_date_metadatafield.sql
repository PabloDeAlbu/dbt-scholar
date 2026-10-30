{{ config(materialized='view') }}

-- ¿Qué valores pueden interpretarse como fechas?
WITH candidate AS (
    SELECT
        item_id,
        metadata_value_id,
        metadatafield_fullname,
        text_value_raw AS value_raw,
        CASE
            WHEN text_value ~ '^[0-9]{4}[-/](0[1-9]|1[0-2])[-/](0[1-9]|[12][0-9]|3[01])'
                THEN TO_DATE(REPLACE(LEFT(text_value, 10), '/', '-'), 'YYYY-MM-DD')
            WHEN text_value ~ '^[0-9]{4}[-/](0[1-9]|1[0-2])$'
                THEN TO_DATE(REPLACE(text_value, '/', '-') || '-01', 'YYYY-MM-DD')
            WHEN text_value ~ '^[0-9]{4}$'
                THEN TO_DATE(text_value || '-01-01', 'YYYY-MM-DD')
        END AS value,
        CASE
            WHEN text_value ~ '^[0-9]{4}[-/](0[1-9]|1[0-2])[-/](0[1-9]|[12][0-9]|3[01])' THEN 'day'
            WHEN text_value ~ '^[0-9]{4}[-/](0[1-9]|1[0-2])$' THEN 'month'
            WHEN text_value ~ '^[0-9]{4}$' THEN 'year'
        END AS date_precision
    FROM {{ ref('int_unlp_sedicidb_item_metadatavalue') }}
    WHERE metadatafield_fullname IN (
        'dc.date.accessioned',
        'dc.date.available',
        'dc.date.issued',
        'sedici.date.exposure'
    )
),

-- ¿Qué fecha se prioriza?
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY item_id, metadatafield_fullname
            ORDER BY
                (value IS NOT NULL) DESC,
                metadata_value_id DESC
        ) AS value_rank
    FROM candidate
),

-- ¿Qué fechas tiene cada metadatafield?
aggregated AS (
    SELECT
        item_id,
        metadatafield_fullname,
        ARRAY_AGG(value ORDER BY metadata_value_id)
            FILTER (WHERE value IS NOT NULL) AS values,
        COUNT(*)::int AS observation_count,
        COUNT(value)::int AS value_count,
        COUNT(DISTINCT value)::int AS distinct_value_count
    FROM candidate
    GROUP BY
        item_id,
        metadatafield_fullname
)

SELECT
    aggregated.item_id,
    aggregated.metadatafield_fullname,
    ranked.metadata_value_id,
    ranked.value_raw,
    ranked.value,
    ranked.date_precision,
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
