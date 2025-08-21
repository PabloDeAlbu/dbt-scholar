{{ config(materialized='table') }}

WITH item AS (
    SELECT 
        item_hk,
        item_uuid
    FROM {{ref('fct_dspace_item')}} 
    WHERE in_archive = True AND withdrawn = False AND discoverable = True
),

dcterms_identifier_other AS (
    SELECT 
        i.item_hk, 
        i.item_uuid, 
        mv.text_value as doi
    FROM item i
    INNER JOIN {{ ref('brg_dspace_item_metadatavalue') }} b ON 
        i.item_hk = b.item_hk
    INNER JOIN {{ ref('dim_dspace_metadatavalue') }} mv ON 
        b.metadatavalue_hk = mv.metadatavalue_hk
    WHERE b.metadatafield_fullname = 'dcterms.identifier.other' and mv.text_value ilike '%doi%'
),

dcterms_identifier_url AS (
    SELECT 
        i.item_hk, 
        i.item_uuid, 
        mv.text_value as doi
    FROM item i
    INNER JOIN {{ ref('brg_dspace_item_metadatavalue') }} b ON 
        i.item_hk = b.item_hk
    INNER JOIN {{ ref('dim_dspace_metadatavalue') }} mv ON 
        b.metadatavalue_hk = mv.metadatavalue_hk
    WHERE b.metadatafield_fullname = 'dcterms.identifier.url' and mv.text_value ilike '%doi%'
),

item_doi AS (
    {# 
    Analizar si incluir dcterms identifier other
    SELECT 
        * 
    FROM dcterms_identifier_other

    UNION  #}
    
    SELECT 
        * 
    FROM dcterms_identifier_url
),

item_type AS (
    SELECT i.item_uuid, doi, mv.text_value as type
    FROM item_doi i
    INNER JOIN {{ ref('brg_dspace_item_metadatavalue') }} b ON 
        i.item_hk = b.item_hk
    INNER JOIN {{ ref('dim_dspace_metadatavalue') }} mv ON 
        b.metadatavalue_hk = mv.metadatavalue_hk
    WHERE b.metadatafield_fullname = 'dc.type'
),

ir_items AS (
    SELECT 
        CONCAT('10.', split_part(doi, '10.', 2)) as doi,
        type 
    FROM item_type
--ORDER BY RANDOM()
--LIMIT 3000
),

openalex_works AS (
    SELECT 
        CONCAT('10.', split_part(doi, '10.', 2)) as doi,
        type 
    FROM {{ ref('fct_openalex_work') }} 
)

SELECT 
* 
FROM openalex_works
ORDER BY RANDOM()
