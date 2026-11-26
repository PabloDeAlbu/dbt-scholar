-- ¿Qué atributos y cobertura de ORCID describe cada autor de OpenAlex?
WITH final AS (
    SELECT
        REPLACE(author_id, 'https://openalex.org/', '') AS author_id,
        display_name,
        works_count,
        cited_by_count,
        orcid,
        orcid_count,
        has_orcid,
        has_single_orcid
    FROM {{ ref('dim_openalex_author') }}
)

SELECT *
FROM final
