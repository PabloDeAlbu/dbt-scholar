{{ config(materialized='view') }}

WITH author_in_scope AS (
    SELECT
        author_id,
        MIN(author_name) AS author_name,
        MIN(author_name_normalized) AS author_name_normalized,
        MIN(author_identity_basis) AS identity_basis,
        MIN(authority_uri) AS authority_uri,
        BOOL_OR(has_authority_control) AS has_authority_control,
        MIN(voc_person_node_id) AS voc_person_node_id,
        BOOL_OR(has_voc_match) AS has_voc_match,
        MAX(sedici_publication_count) AS sedici_publication_count,
        MAX(journal_publication_count) AS journal_publication_count,
        MAX(journal_article_count) AS journal_article_count,
        COUNT(DISTINCT journal_id)::bigint AS journal_count,
        MIN(publication_year) AS first_publication_year,
        MAX(publication_year) AS last_publication_year
    FROM {{ ref('unlp_portalderevistas_00_base') }}
    WHERE is_article IS TRUE
      AND author_id IS NOT NULL
    GROUP BY author_id
),

author_prepared AS (
    SELECT
        author.author_id,
        scope.author_name,
        scope.author_name_normalized,
        scope.identity_basis,
        scope.authority_uri,
        author.authority_host,
        scope.has_authority_control,
        scope.voc_person_node_id,
        scope.has_voc_match,
        author.observed_name_variant_count,
        scope.sedici_publication_count,
        scope.journal_publication_count,
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
