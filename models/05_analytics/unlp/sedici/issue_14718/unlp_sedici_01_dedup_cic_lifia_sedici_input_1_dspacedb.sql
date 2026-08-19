SELECT
    source,
    source_system,
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
    item_url,
    owning_collection_id,
    owning_collection_title,
    owning_community_title
FROM {{ ref('fct_cic_dspacedb_dedup_publication') }}
WHERE dedup_eligible
  AND owning_collection_id IN (
      '3da12564-7da1-4d08-9037-f706cc294a09',
      'ff4e8e87-449a-4df0-b424-07ef1b533e1e'
  )
