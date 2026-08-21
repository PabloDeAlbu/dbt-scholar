{{ config(materialized='view') }}

WITH author_in_scope AS (
    SELECT
        author_id,
        MIN(voc_person_node_id) AS voc_person_node_id,
        BOOL_OR(has_voc_match) AS has_voc_match,
        COUNT(DISTINCT item_id)::bigint AS journal_article_count,
        COUNT(DISTINCT journal_id)::bigint AS journal_count,
        MIN(publication_year) AS first_publication_year,
        MAX(publication_year) AS last_publication_year
    FROM {{ ref('brg_libros_unlp_journal_item_author') }}
    WHERE is_article IS TRUE
    GROUP BY author_id
),

author_prepared AS (
    SELECT
        author.author_id,
        author.author_name_preferred AS author_name,
        author.author_name_normalized,
        CASE
            WHEN author.has_authority_control THEN 'authority'
            ELSE 'normalized_name'
        END::text AS identity_basis,
        author.authority_uri,
        author.authority_host,
        author.has_authority_control,
        scope.voc_person_node_id,
        scope.has_voc_match,
        author.observed_name_variant_count,
        author.item_count AS sedici_item_count,
        scope.journal_article_count,
        scope.journal_count,
        scope.first_publication_year,
        scope.last_publication_year
    FROM author_in_scope AS scope
    INNER JOIN {{ ref('dim_unlp_sedicidb_author') }} AS author
        USING (author_id)
),

final AS (
    SELECT
        *,
        COUNT(*) OVER (
            PARTITION BY author_name_normalized
        )::bigint AS normalized_name_identity_count
    FROM author_prepared
)

SELECT
    *,
    normalized_name_identity_count > 1 AS has_shared_normalized_name
FROM final
