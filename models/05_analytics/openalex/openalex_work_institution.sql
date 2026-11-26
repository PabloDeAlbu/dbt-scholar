
{{ config(materialized = 'table') }}

-- ¿Qué relaciones entre works e instituciones conserva OpenAlex?
WITH work_institution AS (
    SELECT
        work_hk,
        institution_hk
    FROM {{ ref('brg_openalex_work_institution') }}
),

-- ¿Qué ROR están relacionados con cada institución de OpenAlex?
institution_ror AS (
    SELECT
        institution_hk,
        ror
    FROM {{ ref('brg_openalex_institution_ror') }}
),

-- ¿Qué atributos expone cada relación work–institución–ROR?
final AS (
    SELECT
        REPLACE(work.work_id, 'https://openalex.org/', '') AS work_id,
        REPLACE(institution.institution_id, 'https://openalex.org/', '') AS institution_id,
        COALESCE(REPLACE(institution_ror.ror, 'https://ror.org/', ''), '-') AS ror,
        institution.institution_display_name,
        work_institution.institution_hk,
        work_institution.work_hk
    FROM work_institution
    INNER JOIN {{ ref('fct_openalex_work_publication') }} work
        USING (work_hk)
    INNER JOIN {{ ref('dim_openalex_institution') }} institution
        USING (institution_hk)
    LEFT JOIN institution_ror
        USING (institution_hk)
)

SELECT *
FROM final
