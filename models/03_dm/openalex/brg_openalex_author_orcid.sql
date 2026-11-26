{{ config(materialized='table') }}

-- ¿Qué relaciones entre autores y ORCID conserva OpenAlex?
WITH relationship AS (
    SELECT
        author_orcid_hk,
        author_hk,
        orcid_hk
    FROM {{ ref('link_openalex_author_orcid') }}
),

-- ¿Qué valor original y normalizado corresponde a cada ORCID relacionado?
orcid AS (
    SELECT
        orcid_hk,
        orcid AS orcid_raw,
        REPLACE(orcid, 'https://orcid.org/', '') AS orcid
    FROM {{ ref('hub_openalex_orcid') }}
    WHERE orcid NOT IN ('!UNKNOWN', '-')
      AND orcid != ''
),

-- ¿Qué ORCID están asociados con cada autor de OpenAlex?
final AS (
    SELECT
        relationship.author_orcid_hk,
        relationship.author_hk,
        relationship.orcid_hk,
        orcid.orcid_raw,
        orcid.orcid
    FROM relationship
    INNER JOIN orcid
        USING (orcid_hk)
)

SELECT *
FROM final
