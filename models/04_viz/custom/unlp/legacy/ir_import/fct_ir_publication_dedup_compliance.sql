WITH title AS (
    SELECT
        bridge_i_mv.item_hk,
        mv.text_value
    FROM {{ ref('brg_dspace5_item_metadatavalue') }} bridge_i_mv
    INNER JOIN {{ ref('dim_dspace5_metadatavalue') }} mv ON 
        mv.metadatavalue_hk = bridge_i_mv.metadatavalue_hk
    WHERE
        bridge_i_mv.metadatafield_fullname = 'dc.title'
),

id AS (
    SELECT
        bridge_i_mv.item_hk,
        mv.text_value
    FROM {{ ref('brg_dspace5_item_metadatavalue') }} bridge_i_mv
    INNER JOIN {{ ref('dim_dspace5_metadatavalue') }} mv ON 
        mv.metadatavalue_hk = bridge_i_mv.metadatavalue_hk
    WHERE
        bridge_i_mv.metadatafield_fullname = 'dc.identifier.uri' AND
        mv.text_value LIKE '%10915%'
),

type AS (
    SELECT
        bridge_i_mv.item_hk,
        mv.text_value
    FROM {{ ref('brg_dspace5_item_metadatavalue') }} bridge_i_mv
    INNER JOIN {{ ref('dim_dspace5_metadatavalue') }} mv ON 
        mv.metadatavalue_hk = bridge_i_mv.metadatavalue_hk
    WHERE
        bridge_i_mv.metadatafield_fullname = 'dc.type'
),

subject AS (
    SELECT
        bridge_i_mv.item_hk,
        STRING_AGG(mv.text_value, '|' ORDER BY mv.text_value) as text_value
    FROM {{ ref('brg_dspace5_item_metadatavalue') }} bridge_i_mv
    INNER JOIN {{ ref('dim_dspace5_metadatavalue') }} mv ON 
        mv.metadatavalue_hk = bridge_i_mv.metadatavalue_hk
    WHERE
        bridge_i_mv.metadatafield_fullname = 'dc.subject'
    GROUP BY bridge_i_mv.item_hk
),

author AS (
    SELECT 
        bridge_i_mv.item_hk,
        STRING_AGG(mv.text_value, '|' ORDER BY mv.text_value) as text_value
    FROM {{ ref('brg_dspace5_item_metadatavalue') }} bridge_i_mv
    INNER JOIN {{ ref('dim_dspace5_metadatavalue') }} mv ON 
        mv.metadatavalue_hk = bridge_i_mv.metadatavalue_hk
    WHERE bridge_i_mv.metadatafield_fullname = 'sedici.creator.person'
    GROUP BY bridge_i_mv.item_hk
),

issn AS (
  SELECT 
    bridge_i_mv.item_hk,
    STRING_AGG(mv.text_value, '|' ORDER BY mv.text_value) as text_value
  FROM {{ ref('brg_dspace5_item_metadatavalue') }} bridge_i_mv
  JOIN {{ ref('dim_dspace5_metadatavalue') }} mv 
    ON mv.metadatavalue_hk = bridge_i_mv.metadatavalue_hk
  WHERE bridge_i_mv.metadatafield_fullname = 'sedici.identifier.issn'
  GROUP BY bridge_i_mv.item_hk
),

isbn AS (
  SELECT 
    bridge_i_mv.item_hk,
    STRING_AGG(mv.text_value, '|' ORDER BY mv.text_value) as text_value
  FROM {{ ref('brg_dspace5_item_metadatavalue') }} bridge_i_mv
  JOIN {{ ref('dim_dspace5_metadatavalue') }} mv 
    ON mv.metadatavalue_hk = bridge_i_mv.metadatavalue_hk
  WHERE bridge_i_mv.metadatafield_fullname = 'sedici.identifier.isbn'
  GROUP BY bridge_i_mv.item_hk
),

date AS (
  SELECT 
    bridge_i_mv.item_hk,
    STRING_AGG(mv.text_value, '|' ORDER BY mv.text_value) as text_value
  FROM {{ ref('brg_dspace5_item_metadatavalue') }} bridge_i_mv
  JOIN {{ ref('dim_dspace5_metadatavalue') }} mv 
    ON mv.metadatavalue_hk = bridge_i_mv.metadatavalue_hk
  WHERE bridge_i_mv.metadatafield_fullname = 'dc.date.issued'
  GROUP BY bridge_i_mv.item_hk
),

doi_in_dc_identifier_uri AS (
    SELECT
        bridge_i_mv.item_hk,
        mv.text_value
    FROM {{ ref('brg_dspace5_item_metadatavalue') }} bridge_i_mv
    INNER JOIN {{ ref('dim_dspace5_metadatavalue') }} mv ON 
        mv.metadatavalue_hk = bridge_i_mv.metadatavalue_hk
    WHERE
        bridge_i_mv.metadatafield_fullname = 'dc.identifier.uri' AND
        mv.text_value ILIKE '%doi%'
),

doi_in_sedici_identifier_other AS (
    SELECT
        bridge_i_mv.item_hk,
        mv.text_value
    FROM {{ ref('brg_dspace5_item_metadatavalue') }} bridge_i_mv
    INNER JOIN {{ ref('dim_dspace5_metadatavalue') }} mv ON 
        mv.metadatavalue_hk = bridge_i_mv.metadatavalue_hk
    WHERE
        bridge_i_mv.metadatafield_fullname = 'sedici.identifier.other' AND
        mv.text_value ILIKE '%doi%'
),

doi AS (
    SELECT * 
    FROM doi_in_dc_identifier_uri
    UNION 
    SELECT * 
    FROM doi_in_sedici_identifier_other
),

final as (
    SELECT 
        item.item_hk, 
        title.text_value as title,
        id.text_value as id,
        date.text_value as date,
        subject.text_value as subject,
        type.text_value as type,
        author.text_value as author,
        issn.text_value as issn,
        isbn.text_value as isbn,
        doi.text_value as doi
    FROM {{ref('hub_dspace5_item')}} item
    INNER JOIN title ON title.item_hk = item.item_hk
    INNER JOIN id ON id.item_hk = item.item_hk
    INNER JOIN type ON type.item_hk = item.item_hk
    INNER JOIN date ON date.item_hk = item.item_hk
    LEFT JOIN subject ON subject.item_hk = item.item_hk
    LEFT JOIN author ON author.item_hk = item.item_hk
    LEFT JOIN issn ON issn.item_hk = item.item_hk
    LEFT JOIN isbn ON isbn.item_hk = item.item_hk
    LEFT JOIN doi ON doi.item_hk = item.item_hk
)

SELECT * FROM final
