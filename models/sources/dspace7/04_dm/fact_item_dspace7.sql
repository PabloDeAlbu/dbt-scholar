{{ config(materialized='table') }}

WITH base AS (
    SELECT i.* 
    FROM {{ ref('hub_dspace7_dspaceobject') }} i
)

SELECT * FROM base