{{ config(materialized='table') }}

WITH metadata_value AS (
    SELECT
        item_id,
        metadata_value_id,
        metadatafield_fullname,
        text_value
    FROM {{ ref('int_unlp_sedicidb_item_metadatavalue') }}
    WHERE metadatafield_fullname IN (
        'dc.identifier',
        'dc.identifier.other',
        'dc.identifier.uri',
        'sedici.identifier.doi',
        'sedici.identifier.other',
        'sedici.identifier.uri'
    )
      AND text_value IS NOT NULL
),

doi_observation AS (
    SELECT
        item_id,
        metadata_value_id,
        metadatafield_fullname,
        REGEXP_REPLACE(
            SUBSTRING(
                LOWER(text_value)
                FROM '(10[.][0-9]{4,9}/[^[:space:]<>|";]+)'
            ),
            '[.),;:]+$',
            ''
        ) AS doi
    FROM metadata_value
    WHERE SUBSTRING(
        LOWER(text_value)
        FROM '(10[.][0-9]{4,9}/[^[:space:]<>|";]+)'
    ) IS NOT NULL
)

SELECT
    item_id,
    doi,
    STRING_AGG(
        DISTINCT metadatafield_fullname,
        '|'
        ORDER BY metadatafield_fullname
    ) AS metadatafields,
    COUNT(DISTINCT metadata_value_id)::int AS observation_count
FROM doi_observation
GROUP BY
    item_id,
    doi
