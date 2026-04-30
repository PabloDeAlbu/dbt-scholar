{{ config(materialized='table') }}

WITH institution AS (
    SELECT
        ror,
        institution_display_name
    FROM {{ ref('dim_openalex_institution') }}
    WHERE ror <> '-'
),

base AS (
    SELECT
        sat.extract_cdk,
        hub.work_id,
        sat._filter_param,
        sat._filter_value,
        CASE
            WHEN sat._filter_param = 'institutions.ror'
                THEN institution.institution_display_name
        END AS _filter_value_label,
        sat.extract_datetime,
        sat.load_datetime,
        sat.source,
        sat.work_hk
    FROM {{ ref('sat_openalex_work__extract') }} sat
    INNER JOIN {{ ref('hub_openalex_work') }} hub USING (work_hk)
    LEFT JOIN institution
        ON sat._filter_param = 'institutions.ror'
       AND sat._filter_value = institution.ror
)

SELECT * FROM base
