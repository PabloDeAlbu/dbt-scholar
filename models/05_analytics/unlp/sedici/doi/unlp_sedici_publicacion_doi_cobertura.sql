-- ¿Qué publicaciones se consideran?
WITH published_item AS (
    SELECT
        item_id,
        handle,
        dc_title,
        dc_type,
        sedici_subtype,
        publication_year,
        dc_date_accessioned,
        owning_collection,
        owning_collection_title
    FROM {{ ref('unlp_sedici_publicacion_base') }}
),

-- ¿Qué DOI tiene cada publicación y cuántos aparecen en cada campo de metadatos?
doi_coverage AS (
    SELECT
        item_id,
        COUNT(*)::int AS doi_count,
        STRING_AGG(doi, '|' ORDER BY doi) AS doi_values,
        COUNT(*) FILTER (WHERE has_explicit_doi)::int AS sedici_identifier_doi_count,
        COUNT(*) FILTER (WHERE has_sedici_identifier_other)::int AS sedici_identifier_other_doi_count,
        COUNT(*) FILTER (WHERE has_institutional_doi_uri)::int AS dc_identifier_uri_institutional_doi_count
    FROM {{ ref('unlp_sedici_dedup_publication_doi') }}
    GROUP BY item_id
),

-- ¿Qué cobertura de DOI tiene cada publicación visible de SEDICI?
final AS (
    SELECT
        publication.item_id,
        publication.handle,
        publication.dc_title,
        publication.dc_type,
        publication.sedici_subtype,
        publication.publication_year,
        publication.dc_date_accessioned,
        publication.owning_collection,
        publication.owning_collection_title,
        COALESCE(doi.doi_count, 0) AS doi_count,
        doi.doi_values,
        COALESCE(doi.doi_count, 0) > 0 AS has_doi,
        COALESCE(doi.doi_count, 0) > 1 AS has_multiple_doi,
        COALESCE(doi.sedici_identifier_doi_count, 0) AS sedici_identifier_doi_count,
        COALESCE(doi.sedici_identifier_other_doi_count, 0) AS sedici_identifier_other_doi_count,
        COALESCE(
            doi.dc_identifier_uri_institutional_doi_count,
            0
        ) AS dc_identifier_uri_institutional_doi_count
    FROM published_item AS publication
    LEFT JOIN doi_coverage AS doi
        USING (item_id)
)

SELECT *
FROM final
