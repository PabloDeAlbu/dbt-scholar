{{ config(materialized='view') }}

-- ¿Qué observaciones contiene el snapshot exportado desde Redmine?
WITH source AS (
    SELECT *
    FROM {{ ref('seed_unlp_sedici_dal_redmine_issue_example') }}
),

-- ¿Cómo se representan los issues con tipos utilizables en PostgreSQL?
typed AS (
    SELECT
        extract_date,
        issue_id,
        project_name,
        issue_type,
        NULLIF(BTRIM(parent_issue_id), '') AS parent_issue_id_raw,
        NULLIF(
            SUBSTRING(parent_issue_id FROM '([0-9]+)$'),
            ''
        )::integer AS parent_issue_id,
        NULLIF(BTRIM(parent_issue_subject), '') AS parent_issue_subject,
        status,
        priority,
        subject,
        author_name,
        NULLIF(BTRIM(assignee_name), '') AS assignee_name,
        updated_at_raw,
        {{ parse_redmine_datetime('updated_at_raw') }} AS updated_at,
        NULLIF(BTRIM(category), '') AS category,
        NULLIF(BTRIM(target_version), '') AS target_version,
        completion_percentage,
        created_at_raw,
        {{ parse_redmine_datetime('created_at_raw') }} AS created_at,
        NULLIF(BTRIM(closed_at_raw), '') AS closed_at_raw,
        {{ parse_redmine_datetime('closed_at_raw') }} AS closed_at,
        NULLIF(BTRIM(last_updated_by), '') AS last_updated_by,
        NULLIF(BTRIM(related_requests), '') AS related_requests,
        NULLIF(BTRIM(files), '') AS files,
        NULLIF(BTRIM(complexity), '') AS complexity,
        NULLIF(BTRIM(scanner_type), '') AS scanner_type,
        NULLIF(BTRIM(disassembly_condition), '') AS disassembly_condition,
        NULLIF(BTRIM(resource_type), '') AS resource_type,
        NULLIF(BTRIM(contact_person), '') AS contact_person,
        NULLIF(BTRIM(contact_institution), '') AS contact_institution,
        NULLIF(BTRIM(published_resource_url), '') AS published_resource_url,
        NULLIF(BTRIM(google_drive_url), '') AS google_drive_url,
        returned AS returned_raw,
        COALESCE(returned = 'Sí', FALSE) AS is_returned,
        is_private AS is_private_raw,
        is_private = 'Sí' AS is_private,
        NULLIF(BTRIM(published_resource_url), '') IS NOT NULL
            AS has_published_resource_url,
        NULLIF(BTRIM(google_drive_url), '') IS NOT NULL
            AS has_google_drive_url
    FROM source
),

-- ¿Qué atributos quedan disponibles para analizar cada issue registrado?
final AS (
    SELECT *
    FROM typed
)

SELECT *
FROM final
