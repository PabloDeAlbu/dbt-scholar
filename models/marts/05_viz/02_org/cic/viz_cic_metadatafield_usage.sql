{{ config(materialized='table') }}

WITH base AS (
    SELECT
        *
    FROM {{ ref('fct_dspace_metadatafield_usage') }}
    WHERE scheme != 'eperson' AND scheme != 'dspace'
)

SELECT * FROM base
