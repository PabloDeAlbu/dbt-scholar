{{ config(materialized = 'table') }}

WITH base AS (
    SELECT *
    FROM {{ ref('link_dspace5_item_bundle') }}
)

SELECT * FROM base
