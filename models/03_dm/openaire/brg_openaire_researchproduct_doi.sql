{{ config(materialized='table') }}

WITH doi_observation AS (
    SELECT
        researchproduct_hk,
        pid_hk,
        value AS doi_raw,
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                LOWER(BTRIM(value)),
                '^https?://(dx[.])?doi[.]org/',
                ''
            ),
            '[.),;:]+$',
            ''
        ) AS doi
    FROM {{ ref('brg_openaire_researchproduct_pid') }}
    WHERE LOWER(scheme) = 'doi'
      AND value NOT IN ('!UNKNOWN', '-')
      AND NULLIF(BTRIM(value), '') IS NOT NULL
)

SELECT
    researchproduct_hk,
    doi,
    STRING_AGG(DISTINCT doi_raw, '|' ORDER BY doi_raw) AS doi_raw,
    COUNT(DISTINCT pid_hk)::int AS observation_count
FROM doi_observation
GROUP BY
    researchproduct_hk,
    doi
