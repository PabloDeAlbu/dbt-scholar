{{ config(
    materialized = 'view',
    alias = 'oai_record_publication'
) }}

{% set main_publication_type_share_threshold_pct = 0.5 %}

-- ¿Qué publicaciones componen el universo OAI de CONICET Digital?
WITH base AS (
    SELECT
        record_hk,
        record_id,
        title,
        date_issued,
        valid_date_issued,
        extract_datetime::date AS extract_date,
        publication_type_label_es AS publication_type,
        publication_type_uri,
        access_right_label_es AS access_right,
        dc_identifier_uri,
        dc_relation_doi AS doi,
        subject_area,
        subject_subarea
    FROM {{ ref('fct_conicet_oai_record_publication') }}
),

-- ¿Cuántas publicaciones corresponden a cada tipo documental normalizado?
publication_type_count AS (
    SELECT
        publication_type,
        publication_type_uri,
        COUNT(*) AS publication_count
    FROM base
    GROUP BY publication_type, publication_type_uri
),

-- ¿Qué participación tiene cada tipo documental en el corpus completo?
publication_type_with_share AS (
    SELECT
        *,
        ROUND(
            100.0 * publication_count
            / NULLIF(SUM(publication_count) OVER (), 0),
            3
        ) AS publication_type_share_pct
    FROM publication_type_count
),

-- ¿Qué tipos se muestran individualmente y cuáles se agrupan para su presentación?
publication_type_classification AS (
    SELECT
        *,
        CASE
            WHEN publication_type_uri IS NULL THEN 'SIN TIPO NORMALIZADO'
            WHEN publication_type_share_pct
                >= {{ main_publication_type_share_threshold_pct }}
                THEN publication_type
            ELSE 'OTRAS TIPOLOGÍAS'
        END AS publication_type_group,
        publication_type_uri IS NOT NULL
            AND publication_type_share_pct
                >= {{ main_publication_type_share_threshold_pct }}
            AS is_main_publication_type
    FROM publication_type_with_share
),

-- ¿Qué atributos quedan disponibles para analizar cada publicación de CONICET Digital?
final AS (
    SELECT
        base.*,
        classification.publication_type_share_pct,
        classification.publication_type_group,
        classification.is_main_publication_type
    FROM base
    INNER JOIN publication_type_classification AS classification
        ON classification.publication_type
            IS NOT DISTINCT FROM base.publication_type
       AND classification.publication_type_uri
            IS NOT DISTINCT FROM base.publication_type_uri
)

SELECT *
FROM final
