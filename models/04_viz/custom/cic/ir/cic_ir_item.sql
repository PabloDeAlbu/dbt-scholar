{{ config(materialized='table') }}

WITH title AS (
    {{ join_dspace_item_metadatavalue('dc.title', agg=false) }}
),

id AS (
    {{ join_dspace_item_metadatavalue('dc.identifier.uri', filter="mv.text_value LIKE 'https://digital.cic.gba.gob.ar/handle/11746%'") }}
),
type AS (
    {{ join_dspace_item_metadatavalue('cic.parentType') }}
),
subtype AS (
    {{ join_dspace_item_metadatavalue('dc.type') }}
),

subject AS (
    {{ join_dspace_item_metadatavalue('dcterms.subject') }}
),

author  AS (
    {{ join_dspace_item_metadatavalue('dcterms.creator.author', count=true) }}
),

issn AS (
    {{ join_dspace_item_metadatavalue('dcterms.identifier.other', filter="mv.text_value ILIKE '%issn%'") }}
),

isbn AS (
    {{ join_dspace_item_metadatavalue('dcterms.identifier.other', filter="mv.text_value ILIKE '%isbn%'") }}
),

date AS (
    {{ join_dspace_item_metadatavalue('dcterms.issued') }}
),

doi_in_dcterms_identifier_other AS (
    {{ join_dspace_item_metadatavalue('dcterms.identifier.other', filter="mv.text_value ILIKE '%doi%'", count=true, has_value=true) }}
),

doi_in_dcterms_identifier_url AS (
    {{ join_dspace_item_metadatavalue('dcterms.identifier.url', filter="mv.text_value ILIKE '%doi%'") }}
),
doi AS (
    SELECT * 
    FROM doi_in_dcterms_identifier_other

    {# UNION 
    SELECT * 
    FROM doi_in_dcterms_identifier_url #}
),

final as (
    SELECT 
        item.item_hk, 
        item.in_archive,
        title.text_value as title,
        id.text_value as id,
        {{ str_to_date("date.text_value") }} as date_issued,
        subject.text_value as subject,
        type.text_value as type,
        subtype.text_value as subtype,
        author.text_value as author,
        author.value_count as author_count,
        issn.text_value as issn,
        isbn.text_value as isbn,
        doi.text_value as doi,
        COALESCE(doi.has_value, false) as has_doi
    FROM {{ref('er_dspace_item')}} item
    INNER JOIN title ON title.item_hk = item.item_hk
    INNER JOIN id ON id.item_hk = item.item_hk
    LEFT JOIN type ON type.item_hk = item.item_hk
    LEFT JOIN subtype ON subtype.item_hk = item.item_hk
    INNER JOIN date ON date.item_hk = item.item_hk
    LEFT JOIN subject ON subject.item_hk = item.item_hk
    LEFT JOIN author ON author.item_hk = item.item_hk
    LEFT JOIN issn ON issn.item_hk = item.item_hk
    LEFT JOIN isbn ON isbn.item_hk = item.item_hk
    LEFT JOIN doi ON doi.item_hk = item.item_hk
)

SELECT * FROM final
