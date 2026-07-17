{{ config(
    materialized='table',
    indexes=[
        {'columns': ['item_hk', 'metadatafield_hk'], 'unique': true},
        {'columns': ['item_hk', 'metadatafield_fullname']},
        {'columns': ['metadatafield_fullname', 'item_hk']}
    ],
    post_hook=[
        "analyze {{ this }}"
    ]
) }}

WITH base_metadata AS (
    SELECT
        item_hk,
        item_id,
        source_label,
        institution_ror,
        base_url,
        metadatafield_hk,
        metadatafield_fullname,
        short_id,
        metadata_field_id,
        element,
        qualifier,
        metadata_value_id,
        NULLIF(BTRIM(text_value), '') AS text_value_clean,
        place,
        load_datetime
    FROM {{ ref('brg_dspacedb5_item_metadatavalue') }}
    WHERE item_id <> -1
      AND metadata_field_id <> -1
      AND source_label <> '!UNKNOWN'
      AND institution_ror <> '!UNKNOWN'
      AND base_url <> '!UNKNOWN'
),
value_distinct AS (
    SELECT
        item_hk,
        item_id,
        source_label,
        institution_ror,
        base_url,
        metadatafield_hk,
        metadatafield_fullname,
        short_id,
        metadata_field_id,
        element,
        qualifier,
        text_value_clean,
        MIN(place) AS first_place
    FROM base_metadata
    WHERE text_value_clean IS NOT NULL
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
),
ordered_values AS (
    SELECT
        item_hk,
        item_id,
        source_label,
        institution_ror,
        base_url,
        metadatafield_hk,
        metadatafield_fullname,
        short_id,
        metadata_field_id,
        element,
        qualifier,
        STRING_AGG(text_value_clean, '|' ORDER BY first_place NULLS LAST, text_value_clean) AS ordered_text_values
    FROM value_distinct
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11
),
preferred_values AS (
    SELECT DISTINCT ON (
        item_hk,
        item_id,
        source_label,
        institution_ror,
        base_url,
        metadatafield_hk,
        metadatafield_fullname,
        short_id,
        metadata_field_id,
        element,
        qualifier
    )
        item_hk,
        item_id,
        source_label,
        institution_ror,
        base_url,
        metadatafield_hk,
        metadatafield_fullname,
        short_id,
        metadata_field_id,
        element,
        qualifier,
        text_value_clean AS preferred_text_value
    FROM value_distinct
    ORDER BY
        item_hk,
        item_id,
        source_label,
        institution_ror,
        base_url,
        metadatafield_hk,
        metadatafield_fullname,
        short_id,
        metadata_field_id,
        element,
        qualifier,
        first_place NULLS LAST,
        text_value_clean
),
stats AS (
    SELECT
        item_hk,
        item_id,
        source_label,
        institution_ror,
        base_url,
        metadatafield_hk,
        metadatafield_fullname,
        CASE
            WHEN qualifier IS NOT NULL
                THEN short_id || '.' || element || '.' || qualifier
            ELSE short_id || '.' || element
        END AS metadatafield_name,
        short_id,
        metadata_field_id,
        element,
        qualifier,
        COUNT(*) AS metadata_value_count,
        COUNT(DISTINCT metadata_value_id) AS metadata_value_id_count,
        COUNT(DISTINCT text_value_clean) FILTER (WHERE text_value_clean IS NOT NULL) AS distinct_nonempty_text_value_count,
        MIN(text_value_clean) FILTER (WHERE text_value_clean IS NOT NULL) AS min_text_value,
        MIN(text_value_clean) FILTER (
            WHERE text_value_clean ~ '^[0-9]{4}[-/](0[1-9]|1[0-2])[-/](0[1-9]|[12][0-9]|3[01])'
        ) AS min_ymd_text_value,
        MIN(text_value_clean) FILTER (
            WHERE text_value_clean ~ '^[0-9]{4}[-/](0[1-9]|1[0-2])$'
        ) AS min_ym_text_value,
        MIN(text_value_clean) FILTER (
            WHERE text_value_clean ~ '^[0-9]{4}$'
        ) AS min_year_text_value,
        MIN(load_datetime) AS first_load_datetime,
        MAX(load_datetime) AS last_load_datetime
    FROM base_metadata
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
),
final AS (
    SELECT
        stats.item_hk,
        stats.item_id,
        stats.source_label,
        stats.institution_ror,
        stats.base_url,
        stats.metadatafield_hk,
        stats.metadatafield_fullname,
        stats.metadatafield_name,
        stats.short_id,
        stats.metadata_field_id,
        stats.element,
        stats.qualifier,
        stats.metadata_value_count,
        stats.metadata_value_id_count,
        stats.distinct_nonempty_text_value_count,
        preferred.preferred_text_value,
        stats.min_text_value,
        stats.min_ymd_text_value,
        stats.min_ym_text_value,
        stats.min_year_text_value,
        ordered.ordered_text_values,
        stats.first_load_datetime,
        stats.last_load_datetime
    FROM stats
    LEFT JOIN ordered_values AS ordered
        ON stats.item_hk = ordered.item_hk
       AND stats.item_id = ordered.item_id
       AND stats.source_label = ordered.source_label
       AND stats.institution_ror = ordered.institution_ror
       AND stats.base_url = ordered.base_url
       AND stats.metadatafield_hk = ordered.metadatafield_hk
       AND stats.metadatafield_fullname = ordered.metadatafield_fullname
       AND stats.short_id = ordered.short_id
       AND stats.metadata_field_id = ordered.metadata_field_id
       AND stats.element = ordered.element
       AND stats.qualifier IS NOT DISTINCT FROM ordered.qualifier
    LEFT JOIN preferred_values AS preferred
        ON stats.item_hk = preferred.item_hk
       AND stats.item_id = preferred.item_id
       AND stats.source_label = preferred.source_label
       AND stats.institution_ror = preferred.institution_ror
       AND stats.base_url = preferred.base_url
       AND stats.metadatafield_hk = preferred.metadatafield_hk
       AND stats.metadatafield_fullname = preferred.metadatafield_fullname
       AND stats.short_id = preferred.short_id
       AND stats.metadata_field_id = preferred.metadata_field_id
       AND stats.element = preferred.element
       AND stats.qualifier IS NOT DISTINCT FROM preferred.qualifier
)

SELECT * FROM final
