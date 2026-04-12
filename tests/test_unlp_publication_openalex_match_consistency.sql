SELECT pub.publication_hk
FROM {{ ref('fct_unlp_publication') }} AS pub
LEFT JOIN {{ ref('fct_unlp_openalex_work_publication') }} AS oa
    ON oa.work_hk = pub.openalex_work_hk
WHERE pub.matched_by_unique_doi = TRUE
  AND (
      pub.openalex_work_hk IS NULL
      OR pub.match_doi IS NULL
      OR oa.work_hk IS NULL
  )
