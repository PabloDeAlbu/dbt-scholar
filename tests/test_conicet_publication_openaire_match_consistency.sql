SELECT pub.record_hk
FROM {{ ref('fct_conicet_publication') }} AS pub
LEFT JOIN {{ ref('fct_conicet_openaire_researchproduct_publication') }} AS oa
    ON oa.researchproduct_hk = pub.openaire_researchproduct_hk
WHERE pub.matched_by_unique_original_id = TRUE
  AND (
      pub.openaire_original_id IS NULL
      OR pub.openaire_researchproduct_hk IS NULL
      OR pub.openaire_researchproduct_id IS NULL
      OR oa.researchproduct_hk IS NULL
  )
