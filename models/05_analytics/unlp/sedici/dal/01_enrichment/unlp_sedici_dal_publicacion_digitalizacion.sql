{{ config(materialized='view') }}

-- ¿Qué issues pudieron relacionarse con publicaciones de SEDICI?
WITH issue_publication AS (
    SELECT *
    FROM {{ ref('unlp_sedici_dal_issue_publicacion') }}
),

matched_item AS (
    SELECT DISTINCT sedici_item_id AS item_id
    FROM issue_publication
    WHERE sedici_item_id IS NOT NULL
),

-- ¿Qué evidencia explícita contiene cada ítem en sus metadatos?
item_evidence AS (
    SELECT
        metadata.item_id,
        BOOL_OR(
            metadata.metadatafield_fullname = 'sedici.description.note'
            AND metadata.text_value ~* 'digitaliz'
        ) AS has_item_note_evidence,
        BOOL_OR(
            metadata.metadatafield_fullname = 'dc.format.medium'
            AND metadata.text_value
                ~* 'digitaliz|escáner|escaner|scanner|scanned'
        ) AS has_item_format_evidence,
        STRING_AGG(DISTINCT metadata.text_value, ' | ') FILTER (
            WHERE metadata.metadatafield_fullname = 'sedici.description.note'
              AND metadata.text_value ~* 'digitaliz'
        ) AS item_note_evidence,
        STRING_AGG(DISTINCT metadata.text_value, ' | ') FILTER (
            WHERE metadata.metadatafield_fullname = 'dc.format.medium'
              AND metadata.text_value
                  ~* 'digitaliz|escáner|escaner|scanner|scanned'
        ) AS item_format_evidence
    FROM {{ ref('int_unlp_sedicidb_item_metadatavalue') }} AS metadata
    INNER JOIN matched_item
        USING (item_id)
    WHERE metadata.metadatafield_fullname IN (
        'sedici.description.note',
        'dc.format.medium'
    )
    GROUP BY metadata.item_id
),

matched_collection AS (
    SELECT DISTINCT sedici_owning_collection AS collection_id
    FROM issue_publication
    WHERE sedici_owning_collection IS NOT NULL
),

-- ¿La colección aporta solamente contexto general sobre digitalización?
collection_evidence AS (
    SELECT
        description.collection_id,
        TRUE AS has_collection_description_evidence,
        STRING_AGG(DISTINCT description.value, ' | ')
            AS collection_description_evidence
    FROM {{ ref('int_unlp_sedicidb_collection_dc_description') }} AS description
    INNER JOIN matched_collection
        USING (collection_id)
    WHERE description.value ~* 'digitaliz'
    GROUP BY description.collection_id
),

-- ¿Qué señales independientes están disponibles para cada issue y publicación?
evidence AS (
    SELECT
        issue_publication.*,
        COALESCE(item_evidence.has_item_note_evidence, FALSE)
            AS has_item_note_evidence,
        COALESCE(item_evidence.has_item_format_evidence, FALSE)
            AS has_item_format_evidence,
        COALESCE(
            collection_evidence.has_collection_description_evidence,
            FALSE
        ) AS has_collection_description_evidence,
        COALESCE(
            issue_publication.has_sedici_match
            AND issue_publication.issue_type = 'Recurso Digitalización'
            AND issue_publication.has_published_resource_url,
            FALSE
        ) AS has_redmine_published_resource_evidence,
        COALESCE(
            issue_publication.has_sedici_match
            AND issue_publication.sedici_publication_year < 2000
            AND EXTRACT(YEAR FROM issue_publication.sedici_date_accessioned)::int
                - issue_publication.sedici_publication_year >= 10,
            FALSE
        ) AS has_temporal_hint,
        item_evidence.item_note_evidence,
        item_evidence.item_format_evidence,
        collection_evidence.collection_description_evidence
    FROM issue_publication
    LEFT JOIN item_evidence
        ON item_evidence.item_id = issue_publication.sedici_item_id
    LEFT JOIN collection_evidence
        ON collection_evidence.collection_id
            = issue_publication.sedici_owning_collection
),

-- ¿Cuál es la evidencia principal sin convertir indicios en confirmaciones?
classified AS (
    SELECT
        evidence.*,
        CASE
            WHEN NOT has_sedici_match THEN 'not_applicable'
            WHEN has_item_note_evidence THEN 'item_note'
            WHEN has_redmine_published_resource_evidence
                THEN 'redmine_published_resource'
            WHEN has_item_format_evidence THEN 'item_format'
            WHEN has_collection_description_evidence
                THEN 'collection_description'
            WHEN has_temporal_hint THEN 'temporal_hint'
            ELSE 'none'
        END AS digitalization_evidence_code
    FROM evidence
),

-- ¿Qué casos requieren una revisión de los metadatos de SEDICI?
final AS (
    SELECT
        classified.*,
        has_sedici_match
            AND NOT has_item_note_evidence
            AND (
                has_redmine_published_resource_evidence
                OR has_item_format_evidence
                OR has_collection_description_evidence
                OR has_temporal_hint
            ) AS requires_digitalization_review,
        CASE
            WHEN NOT has_sedici_match OR has_item_note_evidence THEN NULL
            ELSE CONCAT_WS(
                '; ',
                CASE
                    WHEN has_redmine_published_resource_evidence
                        THEN 'Redmine registra un recurso publicado en SEDICI'
                END,
                CASE
                    WHEN has_item_format_evidence
                        THEN '`dc.format.medium` menciona digitalización o escaneo'
                END,
                CASE
                    WHEN has_collection_description_evidence
                        THEN 'la descripción de la colección menciona digitalización'
                END,
                CASE
                    WHEN has_temporal_hint
                        THEN 'publicación anterior a 2000 ingresada al menos diez años después'
                END
            )
        END AS digitalization_review_reason
    FROM classified
)

SELECT *
FROM final
