{{ config(materialized='view') }}

WITH sedici_author_stats AS (
    SELECT
        authorship.author_id,
        COUNT(DISTINCT publication.item_id)::bigint
            AS sedici_publication_count
    FROM {{ ref('brg_unlp_sedicidb_item_author') }} AS authorship
    INNER JOIN {{ ref('fct_unlp_sedicidb_item_publication') }} AS publication
        USING (item_id)
    WHERE authorship.author_role = 'creator_person'
      AND authorship.author_type = 'person'
      AND publication.in_archive IS TRUE
      AND publication.withdrawn IS FALSE
      AND publication.discoverable IS TRUE
      AND publication.handle IS NOT NULL
    GROUP BY authorship.author_id
),

journal_author_stats AS (
    SELECT
        author_id,
        COUNT(DISTINCT item_id)::bigint AS journal_publication_count,
        COUNT(DISTINCT item_id) FILTER (
            WHERE is_article
        )::bigint AS journal_article_count
    FROM {{ ref('brg_unlp_portalderevistas_journal_item_author') }}
    GROUP BY author_id
)

SELECT
    item.item_id,
    item.handle,
    item.item_url,
    item.item_title,
    item.dc_type,
    item.sedici_subtype,
    item.publication_date,
    item.publication_year,
    item.journal_id,
    item.journal_title,
    item.is_closed,
    item.owning_community_path_titles,
    item.is_article,
    item.publication_year IS NOT NULL AS has_publication_year,
    authorship.author_id,
    CASE
        WHEN authorship.author_id IS NOT NULL
            THEN item.item_id::text || '|' || authorship.author_id
    END AS authorship_id,
    authorship.author_name,
    authorship.author_name_normalized,
    authorship.author_place,
    CASE
        WHEN authorship.author_id IS NULL THEN NULL
        WHEN authorship.has_authority_control THEN 'authority'
        ELSE 'normalized_name'
    END::text AS author_identity_basis,
    authorship.authority_uri,
    authorship.author_id IS NOT NULL AS has_recognized_author,
    COALESCE(authorship.has_authority_control, FALSE)
        AS has_authority_control,
    authorship.voc_person_node_id,
    COALESCE(authorship.has_voc_match, FALSE) AS has_voc_match,
    sedici.sedici_publication_count,
    journal.journal_publication_count,
    journal.journal_article_count
FROM {{ ref('fct_unlp_portalderevistas_journal_item') }} AS item
LEFT JOIN {{ ref('brg_unlp_portalderevistas_journal_item_author') }} AS authorship
    USING (item_id)
LEFT JOIN sedici_author_stats AS sedici
    USING (author_id)
LEFT JOIN journal_author_stats AS journal
    USING (author_id)
