WITH base AS (
    SELECT
        record_hk,
        record_id,
        title,
        publication_year,
        publication_type,
        subject_area,
        institutional_uri,
        doi,
        has_doi
    FROM {{ ref('fct_conicet_publication') }}
),

final AS (
    SELECT
        record_hk,
        record_id,
        title,
        publication_year,
        publication_type,
        subject_area,
        institutional_uri,
        doi,
        has_doi,
        CASE
            WHEN has_doi THEN 'Con DOI'
            ELSE 'Sin DOI'
        END AS has_doi_label
    FROM base
)

SELECT * FROM final
