{{ config(materialized = 'table') }}

-- ¿Qué atributos describen el estado vigente de cada autor de OpenAlex?
WITH author AS (
    SELECT
        hub_author.author_id,
        hub_author.author_hk,
        sat_author.display_name,
        sat_author.works_count,
        sat_author.cited_by_count
    FROM {{ref('hub_openalex_author')}} hub_author
    INNER JOIN {{ latest_satellite(ref('sat_openalex_author'), 'author_hk') }} AS sat_author USING (author_hk)
),

-- ¿Cuántos ORCID distintos se relacionan con cada autor de OpenAlex?
author_orcid_count AS (
    SELECT
        author_hk,
        COUNT(DISTINCT orcid) AS orcid_count
    FROM {{ ref('brg_openalex_author_orcid') }}
    GROUP BY author_hk
),

-- ¿Qué ORCID pueden atribuirse sin ambigüedad a un autor de OpenAlex?
author_orcid_unambiguous AS (
    SELECT
        relationship.author_hk,
        relationship.orcid
    FROM {{ ref('brg_openalex_author_orcid') }} relationship
    INNER JOIN author_orcid_count
        USING (author_hk)
    WHERE author_orcid_count.orcid_count = 1
),

-- ¿Qué atributos expone la dimensión de autores de OpenAlex?
final AS (
    SELECT
        author.author_id,
        author.author_hk,
        author.display_name,
        author.works_count,
        author.cited_by_count,
        COALESCE(author_orcid_unambiguous.orcid, '-') AS orcid,
        COALESCE(author_orcid_count.orcid_count, 0) AS orcid_count,
        COALESCE(author_orcid_count.orcid_count, 0) > 0 AS has_orcid,
        COALESCE(author_orcid_count.orcid_count, 0) = 1 AS has_single_orcid
    FROM author
    LEFT JOIN author_orcid_count
        USING (author_hk)
    LEFT JOIN author_orcid_unambiguous
        USING (author_hk)
)

SELECT *
FROM final
