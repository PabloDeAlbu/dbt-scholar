SELECT pub.publication_hk
FROM {{ ref('fct_unlp_publication') }} AS pub
LEFT JOIN {{ ref('fct_unlp_openaire_researchproduct') }} AS oa
    ON oa.researchproduct_hk = pub.openaire_researchproduct_hk
WHERE pub.matched_by_unique_original_id = TRUE
  AND (
      pub.openaire_original_id IS NULL
      OR pub.openaire_researchproduct_hk IS NULL
      OR pub.openaire_researchproduct_id IS NULL
      OR oa.researchproduct_hk IS NULL
  )
