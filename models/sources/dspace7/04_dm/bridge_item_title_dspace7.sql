{{ config(materialized='table') }}

WITH 

{# bridge AS (
    SELECT *
    FROM {{ref('dim_title_dspace7')}}
), #}

fact AS (
    SELECT *
    FROM {{ref('fact_item_dspace7')}}
),

link AS (
    SELECT *
    FROM {{ref('link_dspace7_dspaceobject2metadatavalue')}}
)

SELECT * FROM link