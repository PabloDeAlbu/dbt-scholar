WITH base AS (
    SELECT
        publication_year,
        publication_type,
        subject_area,
        has_doi,
        has_repo_handle,
        has_openaire_doi,
        has_openaire_handle,
        has_openaire_arxiv,
        has_openaire_pmid,
        has_openaire_pmc,
        has_openaire_pmb,
        has_openaire_mag
    FROM {{ ref('viz_conicet_publication_pid_profile') }}
),

pid_presence AS (
    SELECT
        publication_year,
        publication_type,
        subject_area,
        'doi'::text AS pid_type,
        'oai'::text AS pid_source,
        has_doi AS has_pid
    FROM base

    UNION ALL

    SELECT
        publication_year,
        publication_type,
        subject_area,
        'handle'::text AS pid_type,
        'repository'::text AS pid_source,
        has_repo_handle AS has_pid
    FROM base

    UNION ALL

    SELECT
        publication_year,
        publication_type,
        subject_area,
        'doi'::text AS pid_type,
        'openaire'::text AS pid_source,
        has_openaire_doi AS has_pid
    FROM base

    UNION ALL

    SELECT
        publication_year,
        publication_type,
        subject_area,
        'handle'::text AS pid_type,
        'openaire'::text AS pid_source,
        has_openaire_handle AS has_pid
    FROM base

    UNION ALL

    SELECT
        publication_year,
        publication_type,
        subject_area,
        'arxiv'::text AS pid_type,
        'openaire'::text AS pid_source,
        has_openaire_arxiv AS has_pid
    FROM base

    UNION ALL

    SELECT
        publication_year,
        publication_type,
        subject_area,
        'pmid'::text AS pid_type,
        'openaire'::text AS pid_source,
        has_openaire_pmid AS has_pid
    FROM base

    UNION ALL

    SELECT
        publication_year,
        publication_type,
        subject_area,
        'pmc'::text AS pid_type,
        'openaire'::text AS pid_source,
        has_openaire_pmc AS has_pid
    FROM base

    UNION ALL

    SELECT
        publication_year,
        publication_type,
        subject_area,
        'pmb'::text AS pid_type,
        'openaire'::text AS pid_source,
        has_openaire_pmb AS has_pid
    FROM base

    UNION ALL

    SELECT
        publication_year,
        publication_type,
        subject_area,
        'mag_id'::text AS pid_type,
        'openaire'::text AS pid_source,
        has_openaire_mag AS has_pid
    FROM base
),

publication_type_agg AS (
    SELECT
        pid_type,
        pid_source,
        'publication_type'::text AS analysis_dimension,
        COALESCE(publication_type, 'Sin tipo de publicacion')::text AS analysis_value,
        COUNT(*)::bigint AS publication_count,
        SUM(CASE WHEN has_pid THEN 1 ELSE 0 END)::bigint AS publication_with_pid_count,
        SUM(CASE WHEN has_pid THEN 0 ELSE 1 END)::bigint AS publication_without_pid_count
    FROM pid_presence
    GROUP BY 1, 2, 3, 4
),

publication_year_agg AS (
    SELECT
        pid_type,
        pid_source,
        'publication_year'::text AS analysis_dimension,
        COALESCE(publication_year::text, 'Sin anio de publicacion') AS analysis_value,
        COUNT(*)::bigint AS publication_count,
        SUM(CASE WHEN has_pid THEN 1 ELSE 0 END)::bigint AS publication_with_pid_count,
        SUM(CASE WHEN has_pid THEN 0 ELSE 1 END)::bigint AS publication_without_pid_count
    FROM pid_presence
    GROUP BY 1, 2, 3, 4
),

subject_area_agg AS (
    SELECT
        pid_type,
        pid_source,
        'subject_area'::text AS analysis_dimension,
        COALESCE(subject_area, 'Sin area tematica')::text AS analysis_value,
        COUNT(*)::bigint AS publication_count,
        SUM(CASE WHEN has_pid THEN 1 ELSE 0 END)::bigint AS publication_with_pid_count,
        SUM(CASE WHEN has_pid THEN 0 ELSE 1 END)::bigint AS publication_without_pid_count
    FROM pid_presence
    GROUP BY 1, 2, 3, 4
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
        pid_type,
        pid_source,
        analysis_dimension,
        analysis_value,
        publication_count,
        publication_with_pid_count,
        publication_without_pid_count,
        ROUND(
            publication_with_pid_count::numeric / NULLIF(publication_count, 0),
            4
        ) AS publication_with_pid_ratio,
        ROUND(
            publication_without_pid_count::numeric / NULLIF(publication_count, 0),
            4
        ) AS publication_without_pid_ratio
    FROM unioned
)

SELECT * FROM final
