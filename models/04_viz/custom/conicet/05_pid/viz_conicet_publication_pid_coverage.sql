WITH base AS (
    SELECT
        publication_year,
        publication_type,
        subject_area,
        has_doi
    FROM {{ ref('viz_conicet_publication_pid_profile') }}
),

publication_type_agg AS (
    SELECT
        'publication_type'::text AS analysis_dimension,
        COALESCE(publication_type, 'Sin tipo de publicacion')::text AS analysis_value,
        COUNT(*)::bigint AS publication_count,
        SUM(CASE WHEN has_doi THEN 1 ELSE 0 END)::bigint AS publication_with_doi_count,
        SUM(CASE WHEN has_doi THEN 0 ELSE 1 END)::bigint AS publication_without_doi_count
    FROM base
    GROUP BY 1, 2
),

publication_year_agg AS (
    SELECT
        'publication_year'::text AS analysis_dimension,
        COALESCE(publication_year::text, 'Sin anio de publicacion') AS analysis_value,
        COUNT(*)::bigint AS publication_count,
        SUM(CASE WHEN has_doi THEN 1 ELSE 0 END)::bigint AS publication_with_doi_count,
        SUM(CASE WHEN has_doi THEN 0 ELSE 1 END)::bigint AS publication_without_doi_count
    FROM base
    GROUP BY 1, 2
),

subject_area_agg AS (
    SELECT
        'subject_area'::text AS analysis_dimension,
        COALESCE(subject_area, 'Sin area tematica')::text AS analysis_value,
        COUNT(*)::bigint AS publication_count,
        SUM(CASE WHEN has_doi THEN 1 ELSE 0 END)::bigint AS publication_with_doi_count,
        SUM(CASE WHEN has_doi THEN 0 ELSE 1 END)::bigint AS publication_without_doi_count
    FROM base
    GROUP BY 1, 2
),

unioned AS (
    SELECT * FROM publication_type_agg
    UNION ALL
    SELECT * FROM publication_year_agg
    UNION ALL
    SELECT * FROM subject_area_agg
),

final AS (
    SELECT
        analysis_dimension,
        analysis_value,
        publication_count,
        publication_with_doi_count,
        publication_without_doi_count,
        ROUND(
            publication_with_doi_count::numeric / NULLIF(publication_count, 0),
            4
        ) AS publication_with_doi_ratio,
        ROUND(
            publication_without_doi_count::numeric / NULLIF(publication_count, 0),
            4
        ) AS publication_without_doi_ratio
    FROM unioned
)

SELECT * FROM final
