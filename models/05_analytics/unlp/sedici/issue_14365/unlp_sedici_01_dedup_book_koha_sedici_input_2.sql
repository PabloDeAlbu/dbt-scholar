SELECT
    source,
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
WHERE type IN ('book', 'bookpart')
