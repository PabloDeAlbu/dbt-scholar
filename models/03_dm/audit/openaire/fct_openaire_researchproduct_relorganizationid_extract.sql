{{ config(
    materialized='incremental',
    unique_key='extract_hk',
    incremental_strategy='delete+insert',
    on_schema_change='fail'
) }}

WITH base AS (
    SELECT
        extract.extract_hk,
        extract.researchproduct_hk,
        extract.researchproduct_id,
        extract.extract_datetime,
        extract.load_datetime,
        extract.source,
        extract._filter_param,
        extract._filter_value AS organization_ror,
        org.organization_hk,
        org.organization_id,
        org.organization_name,
        org.legalname,
        org.acronym
    FROM {{ ref('fct_openaire_researchproduct_extract') }} extract
    LEFT JOIN {{ ref('dim_openaire_organization') }} org
        ON extract._filter_value = org.organization_ror
    WHERE extract._filter_param = 'relOrganizationId'
    {% if is_incremental() %}
      AND extract.load_datetime > (SELECT COALESCE(MAX(load_datetime), '1900-01-01'::timestamp) FROM {{ this }})
    {% endif %}
)

SELECT * FROM base
