{{ config(materialized = 'view') }}

WITH base AS (
    SELECT
        record_id,
        title,
        date_issued,
        publication_type_label_es AS publication_type,
        access_right_label_es AS access_right,
        dc_relation_doi AS doi,
        subject_area,
        subject_subarea
    FROM {{ ref('fct_conicet_oai_record_publication') }}
)

SELECT *
FROM base
