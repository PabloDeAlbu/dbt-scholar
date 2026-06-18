{{ config(materialized = 'table') }}

WITH base AS (
    SELECT
        record_hk,
        record_id,
        title,
        date_issued,
        valid_date_issued,
        repository_identifier,
        institution_ror,
        dc_type,
        dc_identifier_uri,
        dc_relation_doi,
        doi_audit_string,
        has_doi,
        has_multiple_doi,
        doi_count_check,
        subject_area,
        subject_subarea,
        fos_code_level_1,
        mult_fos_flag
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
        MIN(dim.resource_type_label) AS publication_type,
        MIN(dim.resource_type_uri) AS publication_type_uri,
        COUNT(DISTINCT dim.resource_type_uri) AS publication_type_count,
        (COUNT(DISTINCT dim.resource_type_uri) > 1) AS has_multiple_publication_type
    FROM record_dc_type
    INNER JOIN {{ ref('seed_coar_resource_types2conicet_oai_dc_types') }} seed
        ON record_dc_type.dc_type = seed.record_type
    INNER JOIN {{ ref('dim_resource_type') }} dim
        ON seed.coar_uri = dim.resource_type_uri
    WHERE seed.coar_uri != '#N/A'
    GROUP BY record_dc_type.record_hk
),

dim_access_right AS (
    SELECT
        brg.record_hk,
        MIN(dim.access_right_label) AS access_right,
        MIN(dim.access_right_uri) AS access_right_uri
    FROM {{ ref('brg_oai_record_right') }} brg
    INNER JOIN {{ ref('dim_access_right') }} dim
        USING (dc_right)
    GROUP BY brg.record_hk
),

record_set AS (
    SELECT
        brg.record_hk,
        STRING_AGG(DISTINCT dim.set_id, '|' ORDER BY dim.set_id) AS set_spec
    FROM {{ ref('brg_oai_record_set') }} brg
    INNER JOIN {{ ref('dim_oai_set') }} dim
        USING (set_hk)
    GROUP BY brg.record_hk
),

unique_originalid_match AS (
    SELECT
        record_hk,
        original_id AS openaire_original_id,
        researchproduct_hk,
        researchproduct_id
    FROM {{ ref('brg_conicet_publication_originalid') }}
    WHERE is_unique_match
),

openaire_enrichment AS (
    SELECT
        match.record_hk,
        match.openaire_original_id,
        match.researchproduct_hk AS openaire_researchproduct_hk,
        match.researchproduct_id AS openaire_researchproduct_id,
        publication.publication_date AS openaire_publication_date,
        publication.type AS openaire_type,
        publication.publisher AS openaire_publisher,
        publication.best_access_right AS openaire_access_right,
        publication.best_access_right_uri AS openaire_access_right_uri,
        publication.is_green AS openaire_is_green,
        publication.is_in_diamond_journal AS openaire_is_in_diamond_journal,
        publication.downloads AS openaire_downloads,
        publication.views AS openaire_views,
        publication.citation_class AS openaire_citation_class,
        publication.citation_count AS openaire_citation_count,
        publication.impulse AS openaire_impulse,
        publication.impulse_class AS openaire_impulse_class,
        publication.influence AS openaire_influence,
        publication.influence_class AS openaire_influence_class,
        publication.popularity AS openaire_popularity,
        publication.popularity_class AS openaire_popularity_class,
        publication.has_arxiv AS openaire_has_arxiv,
        publication.has_single_arxiv AS openaire_has_single_arxiv,
        publication.arxiv_count AS openaire_arxiv_count,
        publication.has_doi AS openaire_has_doi,
        publication.has_single_doi AS openaire_has_single_doi,
        publication.doi_count AS openaire_doi_count,
        publication.has_handle AS openaire_has_handle,
        publication.has_single_handle AS openaire_has_single_handle,
        publication.handle_count AS openaire_handle_count,
        publication.has_mag AS openaire_has_mag,
        publication.has_single_mag AS openaire_has_single_mag,
        publication.mag_count AS openaire_mag_count,
        publication.has_pmb AS openaire_has_pmb,
        publication.has_single_pmb AS openaire_has_single_pmb,
        publication.pmb_count AS openaire_pmb_count,
        publication.has_pmc AS openaire_has_pmc,
        publication.has_single_pmc AS openaire_has_single_pmc,
        publication.pmc_count AS openaire_pmc_count,
        publication.has_pmid AS openaire_has_pmid,
        publication.has_single_pmid AS openaire_has_single_pmid,
        publication.pmid_count AS openaire_pmid_count
    FROM unique_originalid_match AS match
    INNER JOIN {{ ref('fct_conicet_openaire_researchproduct_publication') }} AS publication
        USING (researchproduct_hk)
),

fil_enrichment AS (
    SELECT
        record_hk,
        has_fil,
        fil_count,
        fil_author_count,
        has_unlp_fil,
        unlp_fil_count,
        unlp_fil_author_count,
        has_cic_fil,
        cic_fil_count,
        cic_fil_author_count
    FROM {{ ref('brg_conicet_publication_fil') }}
),

final AS (
    SELECT
        base.record_hk,
        base.record_id,
        'oai'::text AS source_system,
        base.title,
        CASE
            WHEN base.valid_date_issued THEN base.date_issued
        END AS publication_date,
        CASE
            WHEN base.valid_date_issued THEN EXTRACT(YEAR FROM base.date_issued)::integer
        END AS publication_year,
        dim_publication_type.publication_type,
        dim_publication_type.publication_type_uri,
        COALESCE(dim_publication_type.publication_type_count, 0) AS publication_type_count,
        COALESCE(dim_publication_type.has_multiple_publication_type, false) AS has_multiple_publication_type,
        dim_access_right.access_right,
        dim_access_right.access_right_uri,
        base.repository_identifier,
        base.institution_ror,
        record_set.set_spec,
        base.dc_identifier_uri AS institutional_uri,
        base.dc_relation_doi AS doi,
        base.doi_audit_string,
        base.has_doi,
        base.has_multiple_doi,
        base.doi_count_check AS doi_count,
        base.subject_area,
        base.subject_subarea,
        base.fos_code_level_1 AS subject_code,
        base.mult_fos_flag AS has_multiple_subject_assignments,
        COALESCE(fil_enrichment.has_fil, false) AS has_fil,
        COALESCE(fil_enrichment.fil_count, 0) AS fil_count,
        COALESCE(fil_enrichment.fil_author_count, 0) AS fil_author_count,
        COALESCE(fil_enrichment.has_unlp_fil, false) AS has_unlp_fil,
        COALESCE(fil_enrichment.unlp_fil_count, 0) AS unlp_fil_count,
        COALESCE(fil_enrichment.unlp_fil_author_count, 0) AS unlp_fil_author_count,
        COALESCE(fil_enrichment.has_cic_fil, false) AS has_cic_fil,
        COALESCE(fil_enrichment.cic_fil_count, 0) AS cic_fil_count,
        COALESCE(fil_enrichment.cic_fil_author_count, 0) AS cic_fil_author_count,
        (openaire_enrichment.openaire_researchproduct_hk IS NOT NULL) AS matched_by_unique_original_id,
        openaire_enrichment.openaire_original_id,
        openaire_enrichment.openaire_researchproduct_hk,
        openaire_enrichment.openaire_researchproduct_id,
        openaire_enrichment.openaire_publication_date,
        openaire_enrichment.openaire_type,
        openaire_enrichment.openaire_publisher,
        openaire_enrichment.openaire_access_right,
        openaire_enrichment.openaire_access_right_uri,
        openaire_enrichment.openaire_is_green,
        openaire_enrichment.openaire_is_in_diamond_journal,
        openaire_enrichment.openaire_downloads,
        openaire_enrichment.openaire_views,
        openaire_enrichment.openaire_citation_class,
        openaire_enrichment.openaire_citation_count,
        openaire_enrichment.openaire_impulse,
        openaire_enrichment.openaire_impulse_class,
        openaire_enrichment.openaire_influence,
        openaire_enrichment.openaire_influence_class,
        openaire_enrichment.openaire_popularity,
        openaire_enrichment.openaire_popularity_class,
        openaire_enrichment.openaire_has_arxiv,
        openaire_enrichment.openaire_has_single_arxiv,
        openaire_enrichment.openaire_arxiv_count,
        openaire_enrichment.openaire_has_doi,
        openaire_enrichment.openaire_has_single_doi,
        openaire_enrichment.openaire_doi_count,
        openaire_enrichment.openaire_has_handle,
        openaire_enrichment.openaire_has_single_handle,
        openaire_enrichment.openaire_handle_count,
        openaire_enrichment.openaire_has_mag,
        openaire_enrichment.openaire_has_single_mag,
        openaire_enrichment.openaire_mag_count,
        openaire_enrichment.openaire_has_pmb,
        openaire_enrichment.openaire_has_single_pmb,
        openaire_enrichment.openaire_pmb_count,
        openaire_enrichment.openaire_has_pmc,
        openaire_enrichment.openaire_has_single_pmc,
        openaire_enrichment.openaire_pmc_count,
        openaire_enrichment.openaire_has_pmid,
        openaire_enrichment.openaire_has_single_pmid,
        openaire_enrichment.openaire_pmid_count
    FROM base
    LEFT JOIN dim_publication_type USING (record_hk)
    LEFT JOIN dim_access_right USING (record_hk)
    LEFT JOIN record_set USING (record_hk)
    LEFT JOIN fil_enrichment USING (record_hk)
    LEFT JOIN openaire_enrichment USING (record_hk)
)

SELECT * FROM final
