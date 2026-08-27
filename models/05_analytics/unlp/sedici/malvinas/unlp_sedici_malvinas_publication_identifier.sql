WITH candidate AS (
    SELECT
        solr.search_resource_id AS item_id,
        publication.url_sedici,
        publication.doi,
        publication.isbn,
        publication.issn
    FROM {{ ref('seed_unlp_sedici_malvinas_solr_item_candidate') }} AS solr
    INNER JOIN {{ ref('unlp_sedici_malvinas_publication_candidate') }} AS publication
        ON publication.url_sedici = 'https://sedici.unlp.edu.ar/handle/' || solr.handle
),

identifier_value_ranked AS (
    SELECT
        candidate.item_id,
        metadata.metadatafield_fullname,
        metadata.text_value,
        metadata.place,
        metadata.metadata_value_id,
        ROW_NUMBER() OVER (
            PARTITION BY
                candidate.item_id,
                metadata.metadatafield_fullname,
                LOWER(metadata.text_value)
            ORDER BY metadata.place NULLS LAST, metadata.metadata_value_id
        ) AS value_rank
    FROM candidate
    INNER JOIN {{ ref('int_unlp_sedicidb_item_metadatavalue') }} AS metadata
        USING (item_id)
    WHERE metadata.metadatafield_fullname IN (
        'dc.identifier',
        'dc.identifier.isbn',
        'dc.identifier.issn',
        'dc.identifier.other',
        'dc.identifier.uri',
        'sedici.identifier.doi',
        'sedici.identifier.isbn',
        'sedici.identifier.issn',
        'sedici.identifier.other',
        'sedici.identifier.uri'
    )
      AND metadata.text_value IS NOT NULL
),

identifier_value AS (
    SELECT
        item_id,
        metadatafield_fullname,
        STRING_AGG(
            text_value,
            '; '
            ORDER BY place NULLS LAST, metadata_value_id
        ) AS identifier_values
    FROM identifier_value_ranked
    WHERE value_rank = 1
    GROUP BY item_id, metadatafield_fullname
),

identifier_pivot AS (
    SELECT
        item_id,
        MAX(identifier_values) FILTER (WHERE metadatafield_fullname = 'dc.identifier') AS dc_identifier,
        MAX(identifier_values) FILTER (WHERE metadatafield_fullname = 'dc.identifier.isbn') AS dc_identifier_isbn,
        MAX(identifier_values) FILTER (WHERE metadatafield_fullname = 'dc.identifier.issn') AS dc_identifier_issn,
        MAX(identifier_values) FILTER (WHERE metadatafield_fullname = 'dc.identifier.other') AS dc_identifier_other,
        MAX(identifier_values) FILTER (WHERE metadatafield_fullname = 'dc.identifier.uri') AS dc_identifier_uri,
        MAX(identifier_values) FILTER (WHERE metadatafield_fullname = 'sedici.identifier.doi') AS sedici_identifier_doi,
        MAX(identifier_values) FILTER (WHERE metadatafield_fullname = 'sedici.identifier.isbn') AS sedici_identifier_isbn,
        MAX(identifier_values) FILTER (WHERE metadatafield_fullname = 'sedici.identifier.issn') AS sedici_identifier_issn,
        MAX(identifier_values) FILTER (WHERE metadatafield_fullname = 'sedici.identifier.other') AS sedici_identifier_other,
        MAX(identifier_values) FILTER (WHERE metadatafield_fullname = 'sedici.identifier.uri') AS sedici_identifier_uri
    FROM identifier_value
    GROUP BY item_id
)

SELECT
    candidate.url_sedici,
    candidate.doi,
    candidate.isbn,
    candidate.issn,
    identifiers.dc_identifier,
    identifiers.dc_identifier_isbn,
    identifiers.dc_identifier_issn,
    identifiers.dc_identifier_other,
    identifiers.dc_identifier_uri,
    identifiers.sedici_identifier_doi,
    identifiers.sedici_identifier_isbn,
    identifiers.sedici_identifier_issn,
    identifiers.sedici_identifier_other,
    identifiers.sedici_identifier_uri
FROM candidate
LEFT JOIN identifier_pivot AS identifiers
    USING (item_id)
