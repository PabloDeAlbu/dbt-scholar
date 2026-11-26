{{ config(materialized = 'table') }}

-- ¿Qué relaciones entre autores e instituciones conserva OpenAlex?
WITH author_institution AS (
    SELECT
        author_hk,
        institution_hk
    FROM {{ ref('brg_openalex_author_institution') }}
),

-- ¿Qué ROR están relacionados con cada institución de OpenAlex?
institution_ror AS (
    SELECT
        institution_hk,
        ror
    FROM {{ ref('brg_openalex_institution_ror') }}
),

-- ¿Qué atributos expone cada relación autor–institución–ROR?
final AS (
    SELECT
        REPLACE(author.author_id, 'https://openalex.org/', '') AS author_id,
        REPLACE(institution.institution_id, 'https://openalex.org/', '') AS institution_id,
        COALESCE(institution_ror.ror, '-') AS ror,
        author.display_name AS author_name,
        institution.institution_display_name AS institution_name,
        author.author_hk,
        institution.institution_hk
    FROM author_institution
    INNER JOIN {{ ref('dim_openalex_author') }} author
        USING (author_hk)
    INNER JOIN {{ ref('dim_openalex_institution') }} institution
        USING (institution_hk)
    LEFT JOIN institution_ror
        USING (institution_hk)
)

SELECT *
FROM final
