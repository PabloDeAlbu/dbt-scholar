{{ config(materialized='table') }}

WITH base AS (
    SELECT
        extract_datetime::date AS extract_date,
        institution_ror,
        organization_name,
        openaire_acronym,
        source_system,
        entity_type,
        repository_identifier,
        filter_param,
        filter_value,
        source_label,
        entity_hk,
        extract_cdk,
        extract_datetime,
        load_datetime
    FROM {{ ref('fct_entity_extract') }}
),

final AS (
    SELECT
        extract_date,
        institution_ror,
        organization_name,
        openaire_acronym,
        source_system,
        entity_type,
        repository_identifier,
        filter_param,
        filter_value,
        source_label,
        COUNT(*) AS extract_row_count,
        COUNT(DISTINCT extract_cdk) AS extraction_count,
        COUNT(DISTINCT entity_hk) AS entity_count,
        MIN(extract_datetime) AS first_extract_datetime,
        MAX(extract_datetime) AS last_extract_datetime,
        MIN(load_datetime) AS first_load_datetime,
        MAX(load_datetime) AS last_load_datetime
    FROM base
    GROUP BY
        extract_date,
        institution_ror,
        organization_name,
        openaire_acronym,
        source_system,
        entity_type,
        repository_identifier,
        filter_param,
        filter_value,
        source_label
)

SELECT * FROM final
