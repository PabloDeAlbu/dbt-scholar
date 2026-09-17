{{ config(materialized='table') }}

SELECT
    link.work_doi_hk,
    link.work_hk,
    link.doi_hk,
    doi.doi AS doi_raw,
    REGEXP_REPLACE(
        REGEXP_REPLACE(
            LOWER(BTRIM(doi.doi)),
            '^https?://(dx[.])?doi[.]org/',
            ''
        ),
        '[.),;:]+$',
        ''
    ) AS doi
FROM {{ ref('link_openalex_work_doi') }} AS link
INNER JOIN {{ ref('hub_openalex_doi') }} AS doi
    USING (doi_hk)
WHERE doi.doi NOT IN ('!UNKNOWN', '-')
  AND NULLIF(BTRIM(doi.doi), '') IS NOT NULL
