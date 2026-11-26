{{ config(materialized='view') }}

-- ¿Qué works fueron recuperados mediante el filtro institutions.ror?
WITH extracted_work AS (
    SELECT
        extraction.extract_cdk,
        extraction.work_hk,
        extraction._filter_value AS institution_ror,
        extraction._filter_value_label AS institution_display_name,
        extraction.extract_datetime::date AS extract_date,
        EXTRACT(YEAR FROM publication.publication_year)::integer AS publication_year
    FROM {{ ref('fct_openalex_work_extraction') }} AS extraction
    LEFT JOIN {{ ref('fct_openalex_work_publication') }} AS publication
        USING (work_hk)
    WHERE extraction._filter_param = 'institutions.ror'
),

-- ¿Qué volumen y rango de años de publicación presenta cada extracción institucional diaria?
final AS (
    SELECT
        extract_date,
        institution_ror,
        COALESCE(MAX(institution_display_name), institution_ror) AS institution_display_name,
        COUNT(DISTINCT extract_cdk) AS extraction_event_count,
        COUNT(DISTINCT work_hk) AS extracted_work_count,
        MIN(publication_year) AS min_publication_year,
        MAX(publication_year) AS max_publication_year
    FROM extracted_work
    GROUP BY
        extract_date,
        institution_ror
)

SELECT *
FROM final
