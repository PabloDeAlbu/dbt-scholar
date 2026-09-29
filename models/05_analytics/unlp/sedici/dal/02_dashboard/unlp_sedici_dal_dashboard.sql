{{ config(materialized='view') }}

-- ¿Qué evidencia se reunió para cada issue del proyecto?
WITH issue AS (
    SELECT *
    FROM {{ ref('unlp_sedici_dal_publicacion_digitalizacion') }}
),

-- ¿Cómo se puede comparar la institución declarada sin ocultar su valor original?
institution AS (
    SELECT
        issue.*,
        NULLIF(
            REGEXP_REPLACE(
                REPLACE(
                    REPLACE(BTRIM(contact_institution), '–', '-'),
                    '—', '-'
                ),
                '\s+',
                ' ',
                'g'
            ),
            ''
        ) AS contact_institution_normalized
    FROM issue
),

-- ¿Qué campos orientan la revisión de los issues y sus publicaciones?
final AS (
    SELECT
        extract_date AS fecha_extraccion,
        issue_id AS id_issue_redmine,
        issue_type AS tipo_issue,
        status AS estado_issue,
        subject AS asunto,
        CASE sedici_match_status
            WHEN 'no_published_resource_url' THEN 'Sin URL publicada'
            WHEN 'published_resource_url_without_sedici_handle'
                THEN 'URL sin handle de SEDICI'
            WHEN 'published_resource_url_handle_exact'
                THEN 'Encontrada en SEDICI por handle'
            WHEN 'sedici_handle_not_found'
                THEN 'Handle no encontrado en la base visible de SEDICI'
        END AS estado_cruce_sedici,
        CASE digitalization_evidence_code
            WHEN 'not_applicable' THEN 'Sin publicación vinculada'
            WHEN 'item_note' THEN 'Declarada en nota del ítem'
            WHEN 'redmine_published_resource'
                THEN 'Registrada en DAL con publicación en SEDICI'
            WHEN 'item_format' THEN 'Indicada en el formato del ítem'
            WHEN 'collection_description'
                THEN 'Mencionada solamente en la colección'
            WHEN 'temporal_hint' THEN 'Solo indicio temporal'
            WHEN 'none' THEN 'Sin evidencia'
        END AS evidencia_digitalizacion,
        has_item_note_evidence AS declarada_digitalizacion_en_sedici,
        requires_digitalization_review AS requiere_revision_digitalizacion,
        digitalization_review_reason AS motivo_revision_digitalizacion,
        sedici_handle AS handle_sedici,
        sedici_publication_url AS url_sedici,
        sedici_title AS titulo_sedici,
        sedici_type AS tipo_publicacion_sedici,
        sedici_subtype AS subtipo_publicacion_sedici,
        sedici_date_issued AS fecha_publicacion_sedici,
        sedici_publication_year AS anio_publicacion_sedici,
        sedici_date_accessioned AS fecha_ingreso_sedici,
        sedici_collection AS coleccion_sedici,
        sedici_community AS comunidad_sedici,
        sedici_community_path AS ruta_comunidades_sedici,
        issues_per_sedici_handle AS issues_mismo_handle,
        item_note_evidence AS nota_digitalizacion_sedici,
        item_format_evidence AS formato_digitalizacion_sedici,
        collection_description_evidence
            AS descripcion_coleccion_digitalizacion,
        priority AS prioridad,
        completion_percentage AS avance_porcentaje,
        complexity AS complejidad,
        scanner_type AS escaner_asignado,
        COALESCE(scanner_type = 'DAL', FALSE) AS es_dal,
        disassembly_condition AS condicion_desarmado,
        resource_type AS tipo_recurso,
        contact_institution AS institucion_contacto,
        contact_institution_normalized AS institucion_contacto_normalizada,
        contact_person AS persona_contacto,
        assignee_name AS responsable,
        author_name AS creado_por,
        last_updated_by AS ultima_actualizacion_por,
        created_at AS fecha_creacion,
        updated_at AS fecha_actualizacion,
        closed_at AS fecha_cierre,
        target_version AS destino_previsto,
        is_returned AS fue_devuelto,
        has_published_resource_url AS tiene_recurso_publicado,
        published_resource_url AS url_recurso_publicado,
        has_google_drive_url AS tiene_carpeta_google_drive,
        parent_issue_id AS id_tarea_padre,
        parent_issue_subject AS asunto_tarea_padre
    FROM institution
)

SELECT *
FROM final
