{{ config(materialized='table') }}

-- ¿Con cuántas instituciones de OpenAlex se relaciona cada ROR?
WITH institution_ror_count AS (
    SELECT
        ror,
        COUNT(DISTINCT institution_hk) AS institution_count
    FROM {{ ref('brg_openalex_institution_ror') }}
    GROUP BY ror
),

-- ¿Qué ROR identifican una única institución de OpenAlex?
institution_ror_unambiguous AS (
    SELECT
        relationship.ror,
        relationship.institution_hk
    FROM {{ ref('brg_openalex_institution_ror') }} relationship
    INNER JOIN institution_ror_count
        USING (ror)
    WHERE institution_ror_count.institution_count = 1
),

-- ¿Qué atributos describen cada institución de OpenAlex?
institution AS (
    SELECT
        institution_hk,
        institution_id,
        institution_display_name
    FROM {{ ref('dim_openalex_institution') }}
),

-- ¿Qué eventos de extracción pueden asociarse sin ambigüedad con una institución?
extraction AS (
    SELECT
        sat.extract_cdk,
        hub.work_id,
        sat._filter_param,
        sat._filter_value,
        institution.institution_hk,
        institution.institution_id,
        institution.institution_display_name AS _filter_value_label,
        sat.extract_datetime,
        sat.load_datetime,
        sat.source,
        sat.work_hk
    FROM {{ ref('sat_openalex_work__extract') }} sat
    INNER JOIN {{ ref('hub_openalex_work') }} hub USING (work_hk)
    LEFT JOIN institution_ror_unambiguous institution_ror
        ON sat._filter_param = 'institutions.ror'
       AND sat._filter_value = institution_ror.ror
    LEFT JOIN institution
        USING (institution_hk)
),

-- ¿Qué atributos expone el hecho de extracciones de works de OpenAlex?
final AS (
    SELECT
        extract_cdk,
        work_id,
        _filter_param,
        _filter_value,
        institution_hk,
        institution_id,
        _filter_value_label,
        extract_datetime,
        load_datetime,
        source,
        work_hk
    FROM extraction
)

SELECT *
FROM final
