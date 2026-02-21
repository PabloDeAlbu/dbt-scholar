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

date_issued AS (
    {{ join_dspace_item_metadatavalue('dcterms.issued') }}
),

date_available AS (
    {{ join_dspace_item_metadatavalue('dc.date.available') }}
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
        {{ str_to_date("date_issued.text_value") }} as date_issued,
        {{ str_to_date("date_available.text_value") }} as date_available,
        subject.text_value as subject,
        type.text_value as type,
        subtype.text_value as subtype,
        author.text_value as author,
        author.value_count as author_count,
        issn.text_value as issn,
        isbn.text_value as isbn,
        doi.text_value as doi,
        COALESCE(doi.has_value, false) as has_doi
    FROM {{ref('fct_dspace_item')}} item
    INNER JOIN title USING (item_hk)
    INNER JOIN id USING (item_hk)
    LEFT JOIN type USING (item_hk)
    LEFT JOIN subtype USING (item_hk)
    INNER JOIN date_issued USING (item_hk)
    LEFT JOIN date_available USING (item_hk)
    LEFT JOIN subject USING (item_hk)
    LEFT JOIN author USING (item_hk)
    LEFT JOIN issn USING (item_hk)
    LEFT JOIN isbn USING (item_hk)
    LEFT JOIN doi USING (item_hk)
)

SELECT * FROM final
