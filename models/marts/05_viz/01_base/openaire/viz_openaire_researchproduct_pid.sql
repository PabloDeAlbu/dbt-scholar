WITH base AS (
  SELECT 
    hub_rp.researchproduct_id,
    CASE WHEN has_arxiv  THEN 1 ELSE 0 END AS arxiv,
    CASE WHEN has_doi    THEN 1 ELSE 0 END AS doi,
    CASE WHEN has_handle THEN 1 ELSE 0 END AS handle,
    CASE WHEN has_mag    THEN 1 ELSE 0 END AS mag,
    CASE WHEN has_pmb    THEN 1 ELSE 0 END AS pmb,
    CASE WHEN has_pmc    THEN 1 ELSE 0 END AS pmc,
    CASE WHEN has_pmid   THEN 1 ELSE 0 END AS pmid
  FROM {{ ref('hub_openaire_researchproduct') }} hub_rp
  LEFT JOIN {{ ref('viz_openaire_researchproduct_pid_arxiv') }} USING (researchproduct_hk)
  LEFT JOIN {{ ref('viz_openaire_researchproduct_pid_doi') }}   USING (researchproduct_hk)
  LEFT JOIN {{ ref('viz_openaire_researchproduct_pid_handle') }} USING (researchproduct_hk)
  LEFT JOIN {{ ref('viz_openaire_researchproduct_pid_mag') }}   USING (researchproduct_hk)
  LEFT JOIN {{ ref('viz_openaire_researchproduct_pid_pmb') }}   USING (researchproduct_hk)
  LEFT JOIN {{ ref('viz_openaire_researchproduct_pid_pmc') }}   USING (researchproduct_hk)
  LEFT JOIN {{ ref('viz_openaire_researchproduct_pid_pmid') }}  USING (researchproduct_hk)
)
SELECT
  COUNT(*)                            AS n_total,
  SUM(arxiv)                          AS n_arxiv,
  SUM(arxiv)::numeric / COUNT(*)      AS pct_arxiv,
  SUM(doi)                            AS n_doi,
  SUM(doi)::numeric   / COUNT(*)      AS pct_doi,
  SUM(handle)                         AS n_handle,
  SUM(handle)::numeric/ COUNT(*)      AS pct_handle,
  SUM(mag)                            AS n_mag,
  SUM(mag)::numeric   / COUNT(*)      AS pct_mag,
  SUM(pmb)                            AS n_pmb,
  SUM(pmb)::numeric   / COUNT(*)      AS pct_pmb,
  SUM(pmc)                            AS n_pmc,
  SUM(pmc)::numeric   / COUNT(*)      AS pct_pmc,
  SUM(pmid)                           AS n_pmid,
  SUM(pmid)::numeric  / COUNT(*)      AS pct_pmid
FROM base
