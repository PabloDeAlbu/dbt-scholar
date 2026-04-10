WITH base AS (
    SELECT
        publication_year,
        publication_type,
        subject_area,
        has_doi
    FROM {{ ref('viz_conicet_publication_pid_profile') }}
),

analysis_dimensions AS (
    SELECT
        dimension.analysis_dimension,
        dimension.analysis_value,
        base.has_doi
    FROM base
    CROSS JOIN LATERAL (
        VALUES
            ('publication_type'::text, COALESCE(publication_type, 'Sin tipo de publicacion')::text),
            ('publication_year'::text, COALESCE(publication_year::text, 'Sin anio de publicacion')),
            ('subject_area'::text, COALESCE(subject_area, 'Sin area tematica')::text)
    ) AS dimension(analysis_dimension, analysis_value)
),

final AS (
    SELECT
        analysis_dimension,
        analysis_value,
        COUNT(*)::bigint AS publication_count,
        SUM(CASE WHEN has_doi THEN 1 ELSE 0 END)::bigint AS publication_with_doi_count,
        SUM(CASE WHEN has_doi THEN 0 ELSE 1 END)::bigint AS publication_without_doi_count,
        ROUND(
            SUM(CASE WHEN has_doi THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*), 0),
            4
        ) AS publication_with_doi_ratio,
        ROUND(
            SUM(CASE WHEN has_doi THEN 0 ELSE 1 END)::numeric / NULLIF(COUNT(*), 0),
            4
        ) AS publication_without_doi_ratio
    FROM analysis_dimensions
    GROUP BY 1, 2
)

SELECT * FROM final
