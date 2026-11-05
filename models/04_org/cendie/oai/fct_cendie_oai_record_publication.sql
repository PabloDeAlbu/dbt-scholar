{{ config(materialized='table') }}

{%- set cendie_repository_identifier = 'ojs.cendie.abc.gob.ar' -%}

-- ¿Qué publicaciones OAI pertenecen al repositorio de CENDIE?
WITH cendie_publication AS (
    SELECT *
    FROM {{ ref('fct_oai_record_publication') }}
    WHERE repository_identifier = '{{ cendie_repository_identifier }}'
),

-- ¿Qué sets identifican la revista y la sección de cada publicación CENDIE?
cendie_journal_set AS (
    SELECT
        publication.record_hk,
        oai_set.set_id,
        oai_set.set_name,
        SPLIT_PART(oai_set.set_id, ':', 1) AS journal_id,
        NULLIF(SPLIT_PART(oai_set.set_id, ':', 2), '') AS section_id
    FROM cendie_publication AS publication
    INNER JOIN {{ ref('brg_oai_record_set') }} AS record_set
        USING (record_hk)
    INNER JOIN {{ ref('dim_oai_set') }} AS oai_set
        USING (set_hk)
    WHERE oai_set.set_id LIKE 'revistaanales:%'
       OR oai_set.set_id LIKE 'abame:%'
),

-- ¿Cómo se resume la pertenencia de cada publicación a una revista y sus secciones?
cendie_journal AS (
    SELECT
        record_hk,
        MIN(journal_id) AS journal_id,
        CASE MIN(journal_id)
            WHEN 'revistaanales' THEN 'Revista Anales de la Educación Común'
            WHEN 'abame' THEN 'Anuario BAME'
        END AS journal_name,
        STRING_AGG(DISTINCT section_id, '|' ORDER BY section_id) AS section_ids,
        STRING_AGG(DISTINCT set_name, '|' ORDER BY set_name) AS section_names,
        COUNT(DISTINCT journal_id)::integer AS journal_count,
        COUNT(DISTINCT section_id)::integer AS section_count
    FROM cendie_journal_set
    GROUP BY record_hk
),

-- ¿Qué atributos expone fct_cendie_oai_record_publication?
final AS (
    SELECT
        publication.record_id,
        publication.title,
        publication.date_issued,
        publication.record_hk,
        journal.journal_id,
        journal.journal_name,
        journal.section_ids,
        journal.section_names,
        journal.journal_count,
        journal.section_count,
        publication._context,
        publication.repository_identifier,
        publication.institution_ror,
        publication.extract_datetime,
        publication.load_datetime,
        publication.dc_type,
        publication.dc_type_count,
        publication.has_multiple_dc_type,
        publication.dc_right,
        publication.dc_right_count,
        publication.has_multiple_dc_right,
        publication.valid_date_issued,
        publication.valid_dc_type,
        publication.valid_dc_right
    FROM cendie_publication AS publication
    INNER JOIN cendie_journal AS journal
        USING (record_hk)
)

SELECT *
FROM final
