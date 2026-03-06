{{ config(materialized='table') }}

WITH base AS (
    SELECT
        item_hk,
        item_id,
        dc_identifier_uri,
        date_issued,
        date_accessioned,
        type,
        in_archive,
        withdrawn,
        discoverable
    FROM {{ ref('fct_unlp_ir_item_publication') }}
    WHERE in_archive = TRUE AND withdrawn = FALSE
),

title AS (
    SELECT
        b.item_hk,
        MIN(mv.text_value)::text AS title
    FROM base b
    JOIN {{ ref('brg_dspace5_item_metadatavalue') }} bridge_i_mv
      ON bridge_i_mv.item_hk = b.item_hk AND bridge_i_mv.metadatafield_fullname = 'dc.title'
    JOIN {{ ref('dim_dspace5_metadatavalue') }} mv USING (metadatavalue_hk)
    GROUP BY 1
),

subtype AS (
    SELECT
        b.item_hk,
        STRING_AGG(DISTINCT mv.text_value::text, '|' ORDER BY mv.text_value::text) AS subtype
    FROM base b
    JOIN {{ ref('brg_dspace5_item_metadatavalue') }} bridge_i_mv
      ON bridge_i_mv.item_hk = b.item_hk AND bridge_i_mv.metadatafield_fullname = 'sedici.subtype'
    JOIN {{ ref('dim_dspace5_metadatavalue') }} mv USING (metadatavalue_hk)
    GROUP BY 1
),

subject AS (
    SELECT
        b.item_hk,
        STRING_AGG(DISTINCT mv.text_value::text, '|' ORDER BY mv.text_value::text) AS subject
    FROM base b
    JOIN {{ ref('brg_dspace5_item_metadatavalue') }} bridge_i_mv
      ON bridge_i_mv.item_hk = b.item_hk AND bridge_i_mv.metadatafield_fullname = 'dc.subject'
    JOIN {{ ref('dim_dspace5_metadatavalue') }} mv USING (metadatavalue_hk)
    GROUP BY 1
),

author AS (
    SELECT
        b.item_hk,
        STRING_AGG(DISTINCT mv.text_value::text, '|' ORDER BY mv.text_value::text) AS author,
        COUNT(DISTINCT NULLIF(TRIM(mv.text_value::text), ''))::int AS author_count
    FROM base b
    JOIN {{ ref('brg_dspace5_item_metadatavalue') }} bridge_i_mv
      ON bridge_i_mv.item_hk = b.item_hk AND bridge_i_mv.metadatafield_fullname = 'sedici.creator.person'
    JOIN {{ ref('dim_dspace5_metadatavalue') }} mv USING (metadatavalue_hk)
    GROUP BY 1
),

issn AS (
    SELECT
        b.item_hk,
        STRING_AGG(DISTINCT mv.text_value::text, '|' ORDER BY mv.text_value::text) AS issn
    FROM base b
    JOIN {{ ref('brg_dspace5_item_metadatavalue') }} bridge_i_mv
      ON bridge_i_mv.item_hk = b.item_hk AND bridge_i_mv.metadatafield_fullname = 'sedici.identifier.issn'
    JOIN {{ ref('dim_dspace5_metadatavalue') }} mv USING (metadatavalue_hk)
    GROUP BY 1
),

isbn AS (
    SELECT
        b.item_hk,
        STRING_AGG(DISTINCT mv.text_value::text, '|' ORDER BY mv.text_value::text) AS isbn
    FROM base b
    JOIN {{ ref('brg_dspace5_item_metadatavalue') }} bridge_i_mv
      ON bridge_i_mv.item_hk = b.item_hk AND bridge_i_mv.metadatafield_fullname = 'sedici.identifier.isbn'
    JOIN {{ ref('dim_dspace5_metadatavalue') }} mv USING (metadatavalue_hk)
    GROUP BY 1
),

doi AS (
    SELECT
        b.item_hk,
        STRING_AGG(DISTINCT mv.text_value::text, '|' ORDER BY mv.text_value::text) AS doi
    FROM base b
    JOIN {{ ref('brg_dspace5_item_metadatavalue') }} bridge_i_mv
      ON bridge_i_mv.item_hk = b.item_hk
     AND bridge_i_mv.metadatafield_fullname IN ('dc.identifier.uri', 'sedici.identifier.other')
    JOIN {{ ref('dim_dspace5_metadatavalue') }} mv USING (metadatavalue_hk)
    WHERE mv.text_value ILIKE '%doi%'
    GROUP BY 1
),

final AS (
    SELECT
        b.item_hk,
        b.item_id,
        t.title,
        b.dc_identifier_uri AS id,
        b.date_issued,
        b.date_accessioned,
        s.subject,
        b.type,
        st.subtype,
        a.author,
        COALESCE(a.author_count, 0) AS author_count,
        i.issn,
        ib.isbn,
        d.doi,
        b.discoverable
    FROM base b
    LEFT JOIN title t USING (item_hk)
    LEFT JOIN subject s USING (item_hk)
    LEFT JOIN subtype st USING (item_hk)
    LEFT JOIN author a USING (item_hk)
    LEFT JOIN issn i USING (item_hk)
    LEFT JOIN isbn ib USING (item_hk)
    LEFT JOIN doi d USING (item_hk)
)

SELECT * FROM final
