{{ config(
    materialized = 'view',
    alias = 'oai_record_publication'
) }}

{% set main_resource_type_share_threshold_pct = 0.5 %}

-- ¿Qué records componen el universo OAI de CONICET Digital?
WITH base AS (
    SELECT
        record_hk,
        record_id,
        title,
        date_issued,
        valid_date_issued,
        extract_datetime::date AS extract_date,
        resource_type_label_es AS resource_type,
        resource_type_uri,
        access_right_label_es AS access_right,
        dc_identifier_uri,
        dc_relation_doi AS doi,
        subject_area,
        subject_subarea
    FROM {{ ref('fct_conicet_oai_record_publication') }}
),

-- ¿Cuántos records corresponden a cada tipo de recurso normalizado?
resource_type_count AS (
    SELECT
        resource_type,
        resource_type_uri,
        COUNT(*) AS record_count
    FROM base
    GROUP BY resource_type, resource_type_uri
),

-- ¿Qué participación tiene cada tipo de recurso en el corpus completo?
resource_type_with_share AS (
    SELECT
        *,
        ROUND(
            100.0 * record_count
            / NULLIF(SUM(record_count) OVER (), 0),
            3
        ) AS resource_type_share_pct
    FROM resource_type_count
),

-- ¿Qué tipos se muestran individualmente y cuáles se agrupan para su presentación?
resource_type_classification AS (
    SELECT
        *,
        CASE
            WHEN resource_type_uri IS NULL THEN 'SIN TIPO NORMALIZADO'
            WHEN resource_type_share_pct
                >= {{ main_resource_type_share_threshold_pct }}
                THEN resource_type
            ELSE 'OTRAS TIPOLOGÍAS'
        END AS resource_type_group,
        resource_type_uri IS NOT NULL
            AND resource_type_share_pct
                >= {{ main_resource_type_share_threshold_pct }}
            AS is_main_resource_type
    FROM resource_type_with_share
),

-- ¿Qué atributos quedan disponibles para analizar cada record OAI de CONICET Digital?
final AS (
    SELECT
        base.*,
        classification.resource_type_share_pct,
        classification.resource_type_group,
        classification.is_main_resource_type
    FROM base
    INNER JOIN resource_type_classification AS classification
        ON classification.resource_type
            IS NOT DISTINCT FROM base.resource_type
       AND classification.resource_type_uri
            IS NOT DISTINCT FROM base.resource_type_uri
)

SELECT *
FROM final
