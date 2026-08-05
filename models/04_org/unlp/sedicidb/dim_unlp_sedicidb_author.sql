{{ config(
    materialized='table',
    indexes=[
        {'columns': ['author_id'], 'unique': true},
        {'columns': ['authority_uri'], 'unique': true},
        {'columns': ['author_name_normalized']}
    ],
    post_hook=[
        "analyze {{ this }}"
    ]
) }}

WITH author_observation AS (
    SELECT *
    FROM {{ ref('brg_unlp_sedicidb_item_author') }}
),

name_variant_stats AS (
    SELECT
        author_bk,
        author_name,
        author_name_raw,
        COUNT(*)::bigint AS observation_count
    FROM author_observation
    GROUP BY author_bk, author_name, author_name_raw
),

preferred_name AS (
    SELECT
        author_bk,
        author_name AS author_name_normalized,
        author_name_raw AS author_name_preferred,
        ROW_NUMBER() OVER (
            PARTITION BY author_bk
            ORDER BY observation_count DESC, author_name, author_name_raw
        ) AS name_rank
    FROM name_variant_stats
),

author_stats AS (
    SELECT
        author_bk,
        MIN(author_id) AS author_id,
        CASE
            WHEN COUNT(DISTINCT author_type) = 1 THEN MIN(author_type)
            ELSE 'mixed'
        END AS author_type,
        STRING_AGG(DISTINCT author_role, '|' ORDER BY author_role) AS author_roles,
        STRING_AGG(
            DISTINCT metadatafield_fullname,
            '|'
            ORDER BY metadatafield_fullname
        ) AS metadatafields,
        MIN(authority_uri) AS authority_uri,
        MIN(authority_host) AS authority_host,
        MIN(authority_path) AS authority_path,
        BOOL_OR(has_authority_control) AS has_authority_control,
        MIN(confidence) AS min_confidence,
        MAX(confidence) AS max_confidence,
        COUNT(*)::bigint AS observation_count,
        COUNT(DISTINCT item_id)::bigint AS item_count,
        COUNT(DISTINCT author_name_raw)::bigint AS observed_name_variant_count
    FROM author_observation
    GROUP BY author_bk
),

final AS (
    SELECT
        stats.author_id,
        stats.author_bk,
        preferred.author_name_preferred,
        preferred.author_name_normalized,
        stats.author_type,
        stats.author_roles,
        stats.metadatafields,
        stats.authority_uri,
        stats.authority_host,
        stats.authority_path,
        stats.has_authority_control,
        stats.min_confidence,
        stats.max_confidence,
        stats.observation_count,
        stats.item_count,
        stats.observed_name_variant_count
    FROM author_stats AS stats
    INNER JOIN preferred_name AS preferred
        ON preferred.author_bk = stats.author_bk
       AND preferred.name_rank = 1
)

SELECT *
FROM final
