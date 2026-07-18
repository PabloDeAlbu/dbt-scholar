WITH base AS (
    SELECT *
    FROM {{ ref('fct_conicet_oai_record_publication') }}
),

final AS (
    SELECT
        COUNT(*) AS publication_count,
        COUNT(*) FILTER (WHERE has_publication_type) AS publication_with_type_count,
        COUNT(*) FILTER (WHERE NOT has_publication_type) AS publication_without_type_count,
        COUNT(*) FILTER (WHERE has_access_right) AS publication_with_access_right_count,
        COUNT(*) FILTER (WHERE NOT has_access_right) AS publication_without_access_right_count,
        COUNT(*) FILTER (WHERE valid_date_issued) AS publication_with_valid_date_count,
        COUNT(*) FILTER (WHERE NOT valid_date_issued) AS publication_without_valid_date_count,
        COUNT(*) FILTER (WHERE has_doi) AS publication_with_doi_count,
        MIN(date_issued) FILTER (WHERE valid_date_issued) AS first_publication_date,
        MAX(date_issued) FILTER (WHERE valid_date_issued) AS last_publication_date
    FROM base
)

SELECT * FROM final
