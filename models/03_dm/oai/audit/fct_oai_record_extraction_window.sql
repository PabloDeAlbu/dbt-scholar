{{ config(materialized='table') }}

WITH base AS (
    SELECT
        record_hk,
        repository_identifier,
        institution_ror,
        MIN(extract_datetime) AS first_extract_datetime,
        MAX(extract_datetime) AS last_extract_datetime,
        MIN(load_datetime) AS first_load_datetime,
        MAX(load_datetime) AS last_load_datetime
    FROM {{ ref('fct_oai_record_extraction') }}
    GROUP BY record_hk, repository_identifier, institution_ror
)

SELECT * FROM base
