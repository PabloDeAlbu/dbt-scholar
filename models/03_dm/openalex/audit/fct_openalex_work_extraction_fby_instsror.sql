{{ config(materialized='view') }}

-- ¿Qué works fueron recuperados mediante un filtro institutions.ror?
WITH base AS (
    SELECT
        extract.work_hk,
        extract._filter_value AS institution_ror,
        pub.publication_date,
        extract.institution_id,
        extract._filter_value_label AS institution_display_name,
        extract.extract_datetime
    FROM {{ ref('fct_openalex_work_extraction') }} extract
    LEFT JOIN {{ ref('fct_openalex_work_publication') }} pub
        USING (work_hk)
    WHERE extract._filter_param = 'institutions.ror'
),

-- ¿Cuántos works se recuperaron por fecha, publicación e institución resuelta?
final AS (
    SELECT
        extract_datetime,
        publication_date,
        institution_id,
        institution_ror,
        institution_display_name,
        COUNT(DISTINCT work_hk) AS work_count
    FROM base
    GROUP BY
        extract_datetime,
        publication_date,
        institution_id,
        institution_ror,
        institution_display_name
)

SELECT *
FROM final
ORDER BY extract_datetime DESC
