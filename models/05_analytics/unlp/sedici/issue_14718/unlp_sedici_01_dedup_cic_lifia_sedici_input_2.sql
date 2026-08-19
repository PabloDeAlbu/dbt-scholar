SELECT
    source,
    'sedicidb'::text AS source_system,
    id,
    title,
    subtitle,
    type,
    author,
    date,
    doi,
    isbn,
    issn,
    description,
    owning_community_path_titles,
    owning_collection_title
FROM {{ ref('fct_unlp_sedicidb_dedup_publication') }}
WHERE title IS NOT NULL
  AND type <> 'unknown'
  AND author IS NOT NULL
  AND date IS NOT NULL
