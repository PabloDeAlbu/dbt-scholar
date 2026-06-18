{{ config(materialized='table') }}

WITH base AS (
    SELECT *
    FROM (
        SELECT
            pub.*,
            ROW_NUMBER() OVER (
                PARTITION BY pub.item_uuid
                ORDER BY
                    pub.last_extract_datetime DESC NULLS LAST,
                    pub.last_load_datetime DESC NULLS LAST,
                    pub.first_extract_datetime DESC NULLS LAST,
                    pub.base_url,
                    pub.source_label
            ) AS rn
        FROM {{ ref('fct_dspacedb_item_publication') }} AS pub
        WHERE pub.institution_ror = 'https://ror.org/02s7sax82'
    ) ranked
    WHERE rn = 1
),

owning_collection AS (
    SELECT
        item_uuid,
        owning_collection_uuid,
        owning_collection_title,
        owning_collection_url,
        owning_community_title,
        owning_community_url
    FROM (
        SELECT
            base.item_uuid,
            col.collection_uuid AS owning_collection_uuid,
            col.collection_title AS owning_collection_title,
            col.collection_url AS owning_collection_url,
            col.community_title AS owning_community_title,
            col.community_url AS owning_community_url,
            ROW_NUMBER() OVER (
                PARTITION BY base.item_uuid
                ORDER BY col.collection_title NULLS LAST, col.collection_uuid
            ) AS rn
        FROM base
        LEFT JOIN {{ ref('dim_dspacedb_collection') }} AS col
            ON base.owning_collection = col.collection_uuid
           AND base.base_url = col.base_url
           AND base.institution_ror = col.institution_ror
    ) ranked
    WHERE rn = 1
),

metadatafield_title AS (
    SELECT
        metadatafield_hk
    FROM {{ ref('fct_dspacedb_item_metadata') }}
    WHERE metadatafield_fullname = 'dc.title'
    GROUP BY 1
),

metadatafield_type AS (
    SELECT
        metadatafield_hk
    FROM {{ ref('fct_dspacedb_item_metadata') }}
    WHERE metadatafield_fullname = 'dc.type'
    GROUP BY 1
),

metadatafield_parent_type AS (
    SELECT
        metadatafield_hk
    FROM {{ ref('fct_dspacedb_item_metadata') }}
    WHERE metadatafield_fullname = 'cic.parentType'
    GROUP BY 1
),

title_agg AS (
    SELECT
        base.item_uuid,
        STRING_AGG(DISTINCT mv.text_value, '|' ORDER BY mv.text_value) AS dc_title
    FROM base
    JOIN {{ ref('fct_dspacedb_item_metadata') }} AS mv
        ON base.item_uuid = mv.item_uuid
    JOIN metadatafield_title AS mft
        ON mv.metadatafield_hk = mft.metadatafield_hk
    WHERE mv.text_value IS NOT NULL
      AND mv.text_value <> '!UNKNOWN'
    GROUP BY base.item_uuid
),

type_agg AS (
    SELECT
        base.item_uuid,
        STRING_AGG(DISTINCT mv.text_value, '|' ORDER BY mv.text_value) AS dc_type
    FROM base
    JOIN {{ ref('fct_dspacedb_item_metadata') }} AS mv
        ON base.item_uuid = mv.item_uuid
    JOIN metadatafield_type AS mft
        ON mv.metadatafield_hk = mft.metadatafield_hk
    WHERE mv.text_value IS NOT NULL
      AND mv.text_value <> '!UNKNOWN'
      AND NULLIF(BTRIM(mv.text_value), '') IS NOT NULL
    GROUP BY base.item_uuid
),

parent_type_agg AS (
    SELECT
        base.item_uuid,
        STRING_AGG(DISTINCT mv.text_value, '|' ORDER BY mv.text_value) AS cic_parent_type
    FROM base
    JOIN {{ ref('fct_dspacedb_item_metadata') }} AS mv
        ON base.item_uuid = mv.item_uuid
    JOIN metadatafield_parent_type AS mfpt
        ON mv.metadatafield_hk = mfpt.metadatafield_hk
    WHERE mv.text_value IS NOT NULL
      AND mv.text_value <> '!UNKNOWN'
      AND NULLIF(BTRIM(mv.text_value), '') IS NOT NULL
    GROUP BY base.item_uuid
),

date_raw AS (
    SELECT
        base.item_uuid,
        mv.metadatafield_fullname,
        STRING_AGG(DISTINCT mv.text_value, '|' ORDER BY mv.text_value) AS raw_value
    FROM base
    JOIN {{ ref('fct_dspacedb_item_metadata') }} AS mv
        ON base.item_uuid = mv.item_uuid
       AND base.institution_ror = mv.institution_ror
    WHERE mv.text_value IS NOT NULL
      AND mv.text_value <> '!UNKNOWN'
      AND mv.metadatafield_fullname IN ('dc.date.available', 'dcterms.issued')
      AND NULLIF(BTRIM(mv.text_value), '') IS NOT NULL
    GROUP BY base.item_uuid, mv.metadatafield_fullname
),

date_available_raw AS (
    SELECT
        item_uuid,
        raw_value AS dc_date_available
    FROM date_raw
    WHERE metadatafield_fullname = 'dc.date.available'
),

date_issued_raw AS (
    SELECT
        item_uuid,
        raw_value AS dcterms_issued
    FROM date_raw
    WHERE metadatafield_fullname = 'dcterms.issued'
),

final AS (
    SELECT
        base.*,
        REGEXP_REPLACE(base.base_url, '/+$', '') || '/items/' || base.item_uuid || '/' AS item_url,
        owning.owning_collection_url,
        owning.owning_collection_title,
        owning.owning_community_title,
        owning.owning_community_url,
        available_raw.dc_date_available,
        issued_raw.dcterms_issued,
        title.dc_title,
        parent_type.cic_parent_type,
        type.dc_type
    FROM base
    LEFT JOIN owning_collection AS owning
        USING (item_uuid)
    INNER JOIN type_agg AS type
        USING (item_uuid)
    LEFT JOIN parent_type_agg AS parent_type
        USING (item_uuid)
    LEFT JOIN date_available_raw AS available_raw
        USING (item_uuid)
    LEFT JOIN date_issued_raw AS issued_raw
        USING (item_uuid)
    LEFT JOIN title_agg AS title
        USING (item_uuid)
)

SELECT * FROM final
