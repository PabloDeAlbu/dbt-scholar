{{ config(materialized='table') }}

WITH 

{# bridge AS (
    SELECT *
    FROM {{ref('dim_title_dspace')}}
), #}

fact AS (
    SELECT *
    FROM {{ref('fact_item_dspace')}}
),

link AS (
    SELECT *
    FROM {{ref('link_dspace_metadatavalue_dspaceobject')}}
)

SELECT * FROM link