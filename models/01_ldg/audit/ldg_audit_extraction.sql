WITH openaire_extract AS (
    SELECT
        'openaire'::text AS source_system,
        'researchproduct'::text AS entity_name,
        extract_cdk,
        researchproduct_hk::text AS entity_hk,
        researchproduct_id::text AS entity_id,
        _filter_param,
        _filter_value,
        extract_datetime,
        load_datetime,
        source
    FROM {{ ref('ldg_audit_openaire_researchproduct_extract') }}
),

openalex_extract AS (
    SELECT
        'openalex'::text AS source_system,
        'work'::text AS entity_name,
        extract_cdk,
        work_hk::text AS entity_hk,
        work_id::text AS entity_id,
        _filter_param,
        _filter_value,
        extract_datetime,
        load_datetime,
        source
    FROM {{ ref('ldg_audit_openalex_work_extract') }}
)

SELECT * FROM openaire_extract
UNION ALL
SELECT * FROM openalex_extract
