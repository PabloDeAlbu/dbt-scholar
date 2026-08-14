WITH sedici_book AS (
    SELECT
        publication.*,
        item.item_id
    FROM {{ ref('fct_unlp_sedicidb_dedup_publication') }} AS publication
    INNER JOIN {{ ref('fct_unlp_sedicidb_item_publication') }} AS item
        ON item.handle = publication.id
    WHERE publication.type = 'book'
),

item_fda_metadata AS (
    SELECT
        item_id,
        STRING_AGG(
            DISTINCT text_value,
            '|'
            ORDER BY text_value
        ) FILTER (
            WHERE metadatafield_fullname = 'mods.originInfo.place'
        ) AS mods_origin_info_place,
        STRING_AGG(
            DISTINCT text_value,
            '|'
            ORDER BY text_value
        ) FILTER (
            WHERE metadatafield_fullname = 'dc.publisher'
        ) AS dc_publisher,
        BOOL_OR(
            metadatafield_fullname = 'mods.originInfo.place'
            AND (
                LOWER(text_value) LIKE '%facultad de artes%'
                OR LOWER(text_value) LIKE '%facultad de bellas artes%'
            )
        ) AS matches_fda_origin,
        BOOL_OR(
            metadatafield_fullname = 'dc.publisher'
            AND (
                LOWER(text_value) LIKE '%facultad de artes%'
                OR LOWER(text_value) LIKE '%facultad de bellas artes%'
            )
        ) AS matches_fda_publisher
    FROM {{ ref('int_unlp_sedicidb_item_metadatavalue') }}
    WHERE metadatafield_fullname IN (
        'mods.originInfo.place',
        'dc.publisher'
    )
      AND text_value IS NOT NULL
    GROUP BY item_id
),

book_with_fda_signals AS (
    SELECT
        sedici_book.source,
        sedici_book.id,
        sedici_book.title,
        sedici_book.subtitle,
        sedici_book.type,
        COALESCE(
            NULLIF(BTRIM(sedici_book.author), ''),
            'AUTOR_NO_INFORMADO_SEDICI_' || REPLACE(sedici_book.id, '/', '_')
        ) AS author,
        sedici_book.date,
        sedici_book.doi,
        sedici_book.isbn,
        sedici_book.issn,
        sedici_book.subject,
        sedici_book.description,
        sedici_book.owning_community_path_titles,
        sedici_book.owning_collection_title,
        item_fda_metadata.mods_origin_info_place,
        item_fda_metadata.dc_publisher,
        (
            LOWER(COALESCE(sedici_book.owning_community_path_titles, '')) LIKE '%facultad de artes%'
            OR LOWER(COALESCE(sedici_book.owning_community_path_titles, '')) LIKE '%facultad de bellas artes%'
        ) AS matches_fda_path,
        COALESCE(item_fda_metadata.matches_fda_origin, FALSE) AS matches_fda_origin,
        COALESCE(item_fda_metadata.matches_fda_publisher, FALSE) AS matches_fda_publisher
    FROM sedici_book
    LEFT JOIN item_fda_metadata
        USING (item_id)
),

book_with_fda_evidence AS (
    SELECT
        *,
        NULLIF(
            CONCAT_WS(
                '|',
                CASE WHEN matches_fda_path THEN 'community_path' END,
                CASE WHEN matches_fda_origin THEN 'mods.originInfo.place' END,
                CASE WHEN matches_fda_publisher THEN 'dc.publisher' END
            ),
            ''
        ) AS fda_evidence
    FROM book_with_fda_signals
)

SELECT *
FROM book_with_fda_evidence
WHERE matches_fda_path
   OR matches_fda_origin
   OR matches_fda_publisher
