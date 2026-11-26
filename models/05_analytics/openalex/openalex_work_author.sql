
{{ config(materialized = 'table') }}

-- ¿Qué atributos de autoría y ORCID pueden resolverse para cada work?
WITH resolved AS (
    SELECT
        REPLACE(work.work_id, 'https://openalex.org/', '') AS work_id,
        REPLACE(COALESCE(relationship.author_id, author.author_id), 'https://openalex.org/', '') AS author_id,
        COALESCE(relationship.canonical_display_name, relationship.fallback_display_name) AS display_name,
        COALESCE(NULLIF(author.orcid, '-'), relationship.fallback_orcid) AS orcid,
        author.works_count,
        author.cited_by_count,
        relationship.work_hk,
        relationship.author_hk
    FROM {{ ref('brg_openalex_work_author') }} AS relationship
    INNER JOIN {{ ref('fct_openalex_work_publication') }} work
        USING (work_hk)
    LEFT JOIN {{ ref('dim_openalex_author') }} author
        USING (author_hk)
    WHERE COALESCE(relationship.canonical_display_name, relationship.fallback_display_name) IS NOT NULL
),

-- ¿Qué atributos expone cada relación entre work y autor?
final AS (
    SELECT
        work_id,
        author_id,
        display_name,
        COALESCE(orcid, '-') AS orcid,
        orcid IS NOT NULL AS has_orcid,
        works_count,
        cited_by_count,
        work_hk,
        author_hk
    FROM resolved
)

SELECT *
FROM final
