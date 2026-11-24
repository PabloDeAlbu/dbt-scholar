{{ config(materialized = 'table') }}

-- ¿Qué creadores poseen información de filiación asociada a un record CONICET?
WITH
creators_base AS (
    SELECT 
        author,
        filliation,
        institution_count,
        record_hk
    FROM {{ ref('brg_conicet_record_fil') }}
    WHERE author IS NOT NULL AND author != ''
),

-- ¿Qué contexto de publicación aporta cada record relacionado con un creador?
publication_context AS (
    SELECT 
        record_hk,
        record_id,
        title,
        publication_date,
        subject_area,
        subject_subarea,
        resource_type,
        has_doi
    FROM {{ ref('fct_conicet_publication') }}
),

-- ¿Qué actividad de publicación se expone para cada relación entre creador y record?
final AS (
    SELECT
        UPPER(REPLACE(creator.author, '.', '')) AS author,
        creator.filliation,
        creator.institution_count,
        publication.record_id,
        publication.title,
        publication.publication_date AS date_issued,
        publication.subject_area,
        publication.subject_subarea,
        publication.resource_type AS coar_type,
        creator.record_hk,
        1 AS activity_count
    FROM creators_base AS creator
    INNER JOIN publication_context AS publication
        USING (record_hk)
)

SELECT *
FROM final
