WITH base AS (
    SELECT
        record_hk,
        record_id,
        title,
        CASE
            WHEN valid_date_issued THEN EXTRACT(YEAR FROM date_issued)::integer
        END AS publication_year,
        dc_type,
        dc_identifier_uri AS institutional_uri,
        dc_relation_doi AS doi,
        has_doi,
        subject_area
    FROM {{ ref('fct_conicet_oai_record_publication') }}
),

record_dc_type AS (
    SELECT DISTINCT
        base.record_hk,
        NULLIF(dc_type_value, '') AS dc_type
    FROM base
    CROSS JOIN LATERAL REGEXP_SPLIT_TO_TABLE(COALESCE(base.dc_type, ''), '[|]') AS dc_type_value
    WHERE NULLIF(dc_type_value, '') IS NOT NULL
),

dim_publication_type AS (
    SELECT
        record_dc_type.record_hk,
        MIN(dim.resource_type_label) AS publication_type
    FROM record_dc_type
    INNER JOIN {{ ref('seed_coar_resource_types2conicet_oai_dc_types') }} AS seed
        ON record_dc_type.dc_type = seed.record_type
    INNER JOIN {{ ref('dim_resource_type') }} AS dim
        ON seed.coar_uri = dim.resource_type_uri
    WHERE seed.coar_uri != '#N/A'
    GROUP BY record_dc_type.record_hk
),

unique_doi_match AS (
    SELECT
        record_hk,
        researchproduct_hk
    FROM {{ ref('brg_conicet_publication_doi') }}
    WHERE is_unique_match
),

final AS (
    SELECT
        base.record_hk,
        base.record_id,
        'oai'::text AS source_system,
        base.title,
        base.publication_year,
        dim_publication_type.publication_type,
        base.subject_area,
        base.institutional_uri,
        base.doi,
        base.has_doi,
        (unique_doi_match.researchproduct_hk IS NOT NULL) AS matched_by_unique_doi,
        (base.institutional_uri IS NOT NULL AND TRIM(base.institutional_uri) <> '') AS has_repo_handle,
        COALESCE(openaire.has_doi, false) AS has_openaire_doi,
        COALESCE(openaire.doi_count, 0) AS openaire_doi_count,
        COALESCE(openaire.has_handle, false) AS has_openaire_handle,
        COALESCE(openaire.handle_count, 0) AS openaire_handle_count,
        COALESCE(openaire.has_arxiv, false) AS has_openaire_arxiv,
        COALESCE(openaire.arxiv_count, 0) AS openaire_arxiv_count,
        COALESCE(openaire.has_pmid, false) AS has_openaire_pmid,
        COALESCE(openaire.pmid_count, 0) AS openaire_pmid_count,
        COALESCE(openaire.has_pmc, false) AS has_openaire_pmc,
        COALESCE(openaire.pmc_count, 0) AS openaire_pmc_count,
        COALESCE(openaire.has_pmb, false) AS has_openaire_pmb,
        COALESCE(openaire.pmb_count, 0) AS openaire_pmb_count,
        COALESCE(openaire.has_mag, false) AS has_openaire_mag,
        COALESCE(openaire.mag_count, 0) AS openaire_mag_count,
        CASE
            WHEN base.has_doi THEN 'Con DOI'
            ELSE 'Sin DOI'
        END AS has_doi_label
    FROM base
    LEFT JOIN dim_publication_type USING (record_hk)
    LEFT JOIN unique_doi_match USING (record_hk)
    LEFT JOIN {{ ref('fct_conicet_openaire_researchproduct_publication') }} AS openaire
        USING (researchproduct_hk)
)

SELECT * FROM final
