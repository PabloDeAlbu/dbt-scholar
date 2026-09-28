-- ¿Qué publicaciones de CONICET Digital se intentan enriquecer con OpenAIRE?
WITH base AS (
    SELECT
        record_hk,
        record_id,
        title,
        CASE WHEN valid_date_issued THEN date_issued END AS publication_date,
        publication_type,
        publication_type_group,
        access_right,
        dc_identifier_uri AS institutional_uri,
        doi,
        subject_area,
        subject_subarea
    FROM {{ ref('conicet_oai_record_publication') }}
),

-- ¿Qué evidencia produjo la correspondencia exacta por identificador original?
match_evidence AS (
    SELECT
        record_hk,
        MIN(original_id) AS openaire_original_id,
        MAX(openaire_match_count) AS openaire_match_count,
        MIN(match_rule) AS openaire_match_rule
    FROM {{ ref('brg_conicet_publication_originalid') }}
    GROUP BY record_hk
),

-- ¿Qué correspondencias resolvieron un único producto de investigación en OpenAIRE?
unique_match AS (
    SELECT
        record_hk,
        researchproduct_hk,
        researchproduct_id
    FROM {{ ref('brg_conicet_publication_originalid') }}
    WHERE is_unique_match
),

-- ¿Qué atributos aporta OpenAIRE para los productos de investigación relacionados?
openaire AS (
    SELECT
        researchproduct_hk,
        conicet_first_load_datetime AS openaire_first_load_datetime,
        conicet_last_load_datetime AS openaire_last_load_datetime,
        best_access_right AS openaire_access_right,
        best_access_right_uri AS openaire_access_right_uri,
        citation_class AS openaire_citation_class,
        citation_count AS openaire_citation_count
    FROM {{ ref('fct_conicet_openaire_researchproduct_publication') }}
),

-- ¿Qué datos institucionales, evidencia y atributos externos reúne cada publicación?
enriched AS (
    SELECT
        base.*,
        COALESCE(match_evidence.openaire_match_count, 0)
            AS openaire_match_count,
        match_evidence.openaire_match_rule,
        match_evidence.openaire_original_id,
        unique_match.researchproduct_id AS openaire_researchproduct_id,
        openaire.openaire_first_load_datetime,
        openaire.openaire_last_load_datetime,
        openaire.openaire_last_load_datetime::date AS openaire_load_date,
        openaire.openaire_access_right,
        openaire.openaire_access_right_uri,
        openaire.openaire_citation_class,
        openaire.openaire_citation_count
    FROM base
    LEFT JOIN match_evidence USING (record_hk)
    LEFT JOIN unique_match USING (record_hk)
    LEFT JOIN openaire USING (researchproduct_hk)
),

-- ¿Cómo se interpretan la correspondencia y la disponibilidad de citas?
classified AS (
    SELECT
        enriched.*,
        CASE
            WHEN openaire_match_count = 1
                THEN 'CON CORRESPONDENCIA ÚNICA'
            WHEN openaire_match_count > 1
                THEN 'CORRESPONDENCIA AMBIGUA'
            ELSE 'SIN CORRESPONDENCIA EXACTA'
        END AS match_status,
        openaire_match_count = 1 AS matched_by_unique_original_id,
        openaire_citation_count IS NOT NULL AS has_openaire_citation_data,
        CASE
            WHEN openaire_match_count <> 1
                THEN 'SIN CORRESPONDENCIA ÚNICA'
            WHEN openaire_citation_count IS NULL
                THEN 'SIN DATOS DE CITAS'
            WHEN openaire_citation_count = 0
                THEN 'SIN CITAS REGISTRADAS'
            ELSE 'CON CITAS REGISTRADAS'
        END AS openaire_citation_status
    FROM enriched
),

-- ¿Qué atributos quedan disponibles para analizar el enriquecimiento con OpenAIRE?
final AS (
    SELECT
        record_id,
        title,
        publication_date,
        publication_type,
        publication_type_group,
        access_right,
        institutional_uri,
        doi,
        subject_area,
        subject_subarea,
        match_status,
        matched_by_unique_original_id,
        openaire_match_count,
        openaire_match_rule,
        openaire_original_id,
        openaire_researchproduct_id,
        openaire_first_load_datetime,
        openaire_last_load_datetime,
        openaire_load_date,
        openaire_access_right,
        openaire_access_right_uri,
        openaire_citation_class,
        has_openaire_citation_data,
        openaire_citation_status,
        openaire_citation_count
    FROM classified
)

SELECT *
FROM final
