{{ config(materialized='view') }}

-- ¿Qué issues fueron registrados en el proyecto de digitalización?
WITH issue AS (
    SELECT *
    FROM {{ ref('unlp_sedici_dal_recurso_base') }}
),

-- ¿Qué handle de SEDICI puede extraerse de la URL publicada en Redmine?
issue_handle AS (
    SELECT
        issue.*,
        SUBSTRING(published_resource_url FROM '(10915/[0-9]+)')
            AS sedici_handle
    FROM issue
),

-- ¿Cuántos issues del snapshot apuntan a cada handle?
issue_handle_count AS (
    SELECT
        issue_handle.*,
        CASE
            WHEN sedici_handle IS NOT NULL THEN COUNT(*) OVER (
                PARTITION BY extract_date, sedici_handle
            )
            ELSE 0
        END AS issues_per_sedici_handle
    FROM issue_handle
),

-- ¿Qué publicación visible de SEDICI corresponde exactamente con ese handle?
publication AS (
    SELECT
        item_id,
        handle,
        dc_identifier_uri,
        dc_title,
        dc_type,
        sedici_subtype,
        dc_date_issued,
        publication_year,
        dc_date_accessioned,
        dc_date_available,
        owning_collection,
        owning_collection_title,
        owning_community_title,
        owning_community_path_titles
    FROM {{ ref('unlp_sedici_publicacion_base') }}
),

-- ¿Cuál fue el resultado del cruce y qué evidencia conserva?
final AS (
    SELECT
        issue_handle_count.*,
        publication.item_id AS sedici_item_id,
        publication.dc_identifier_uri AS sedici_publication_url,
        publication.dc_title AS sedici_title,
        publication.dc_type AS sedici_type,
        publication.sedici_subtype AS sedici_subtype,
        publication.dc_date_issued AS sedici_date_issued,
        publication.publication_year AS sedici_publication_year,
        publication.dc_date_accessioned AS sedici_date_accessioned,
        publication.dc_date_available AS sedici_date_available,
        publication.owning_collection AS sedici_owning_collection,
        publication.owning_collection_title AS sedici_collection,
        publication.owning_community_title AS sedici_community,
        publication.owning_community_path_titles AS sedici_community_path,
        publication.item_id IS NOT NULL AS has_sedici_match,
        CASE
            WHEN NOT has_published_resource_url
                THEN 'no_published_resource_url'
            WHEN sedici_handle IS NULL
                THEN 'published_resource_url_without_sedici_handle'
            WHEN publication.item_id IS NOT NULL
                THEN 'published_resource_url_handle_exact'
            ELSE 'sedici_handle_not_found'
        END AS sedici_match_status
    FROM issue_handle_count
    LEFT JOIN publication
        ON publication.handle = issue_handle_count.sedici_handle
)

SELECT *
FROM final
