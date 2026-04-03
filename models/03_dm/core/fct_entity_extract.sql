{{ config(materialized='table') }}

WITH openalex_extract AS (
    SELECT
        'openalex'::text AS source_system,
        'work'::text AS entity_type,
        extract.work_hk AS entity_hk,
        extract.work_id::text AS entity_id,
        extract.extract_cdk::text AS extract_cdk,
        extract.extract_datetime,
        extract.load_datetime,
        extract.institution_ror,
        NULL::text AS repository_identifier,
        extract._filter_param::text AS filter_param,
        extract.institution_ror::text AS filter_value,
        NULL::text AS source_label
    FROM {{ ref('fct_openalex_work_extraction_fby_instsror') }} extract
),

openaire_extract AS (
    SELECT
        'openaire'::text AS source_system,
        'researchproduct'::text AS entity_type,
        extract.researchproduct_hk AS entity_hk,
        extract.researchproduct_id::text AS entity_id,
        extract.extract_cdk::text AS extract_cdk,
        extract.extract_datetime,
        extract.load_datetime,
        extract.organization_ror AS institution_ror,
        NULL::text AS repository_identifier,
        extract._filter_param::text AS filter_param,
        extract.organization_ror::text AS filter_value,
        NULL::text AS source_label
    FROM {{ ref('fct_openaire_researchproduct_extraction_fby_relorgid') }} extract
),

oai_extract AS (
    SELECT
        'oai'::text AS source_system,
        'record'::text AS entity_type,
        extract.record_hk AS entity_hk,
        extract.record_id::text AS entity_id,
        extract.extract_cdk::text AS extract_cdk,
        extract.extract_datetime,
        extract.load_datetime,
        extract.institution_ror,
        extract.repository_identifier,
        NULL::text AS filter_param,
        NULL::text AS filter_value,
        NULL::text AS source_label
    FROM {{ ref('fct_oai_record_extraction') }} extract
),

dspacedb_extract AS (
    SELECT
        'dspacedb'::text AS source_system,
        'item'::text AS entity_type,
        extract.item_hk AS entity_hk,
        extract.item_uuid::text AS entity_id,
        extract.extract_cdk::text AS extract_cdk,
        extract.extract_datetime,
        extract.load_datetime,
        extract.institution_ror,
        NULL::text AS repository_identifier,
        NULL::text AS filter_param,
        NULL::text AS filter_value,
        extract.source_label::text AS source_label
    FROM {{ ref('fct_dspacedb_item_extraction') }} extract
),

dspacedb5_extract AS (
    SELECT
        'dspacedb5'::text AS source_system,
        'item'::text AS entity_type,
        extract.item_hk AS entity_hk,
        extract.item_id::text AS entity_id,
        extract.extract_cdk::text AS extract_cdk,
        extract.extract_datetime,
        extract.load_datetime,
        extract.institution_ror,
        NULL::text AS repository_identifier,
        NULL::text AS filter_param,
        NULL::text AS filter_value,
        extract.source_label::text AS source_label
    FROM {{ ref('fct_dspacedb5_item_extraction') }} extract
),

unioned AS (
    SELECT * FROM openalex_extract
    UNION ALL
    SELECT * FROM openaire_extract
    UNION ALL
    SELECT * FROM oai_extract
    UNION ALL
    SELECT * FROM dspacedb_extract
    UNION ALL
    SELECT * FROM dspacedb5_extract
),

final AS (
    SELECT
        extract.source_system,
        CASE
            WHEN extract.source_system = 'dspacedb' THEN 'DSpace 7+'
            WHEN extract.source_system = 'dspacedb5' THEN 'DSpace 5'
            WHEN extract.source_system = 'oai' THEN 'OAI-PMH'
            WHEN extract.source_system = 'openaire' THEN 'OpenAIRE'
            WHEN extract.source_system = 'openalex' THEN 'OpenAlex'
        END AS source_label,
        CASE
            WHEN extract.source_system IN ('dspacedb', 'dspacedb5') THEN 'Repositorio Institucional'
            WHEN extract.source_system = 'oai' THEN 'OAI-PMH'
            WHEN extract.source_system = 'openaire' THEN 'OpenAIRE'
            WHEN extract.source_system = 'openalex' THEN 'OpenAlex'
        END AS source_short_label,
        extract.entity_type,
        extract.entity_hk,
        extract.entity_id,
        extract.extract_cdk,
        extract.extract_datetime,
        extract.load_datetime,
        extract.institution_ror,
        org.organization_name,
        org.openaire_organization_id,       
        org.openaire_acronym,
        extract.repository_identifier,
        extract.filter_param,
        extract.filter_value
    FROM unioned extract
    LEFT JOIN {{ ref('dim_organization') }} org
        ON extract.institution_ror = org.organization_ror
)

SELECT * FROM final
