WITH repository AS (
    SELECT
        source,
        id,
        title,
        subtitle,
        type AS type_raw,
        type_dedup AS type,
        author,
        date,
        doi,
        isbn,
        issn,
        description,
        owning_root_community_id,
        owning_root_community_title,
        owning_community_id,
        owning_community_title,
        owning_community_path_titles,
        owning_collection,
        owning_collection_title
    FROM {{ ref('fct_unlp_sedici_dedup_publication') }}
    WHERE dedup_eligible
      AND owning_community_id IS NOT NULL
      AND owning_community_path_titles IS NOT NULL
),

target AS (
    SELECT DISTINCT
        target_community_id,
        target_community_path_titles
    FROM {{ ref('conicet_unlp_repository_target') }}
),

base AS (
    SELECT
        repository.*,
        target.target_community_id,
        target.target_community_path_titles
    FROM repository
    INNER JOIN target
        ON repository.owning_community_path_titles = target.target_community_path_titles
        OR repository.owning_community_path_titles LIKE target.target_community_path_titles || ' > %'
)

SELECT *
FROM base
