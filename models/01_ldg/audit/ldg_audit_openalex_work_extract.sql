WITH base AS (
    SELECT
        sat.extract_cdk,
        sat.work_hk,
        hub.work_id,
        sat._filter_param,
        sat._filter_value,
        sat.extract_datetime,
        sat.load_datetime,
        sat.source
    FROM {{ ref('sat_openalex_work__extract') }} sat
    INNER JOIN {{ ref('hub_openalex_work') }} hub
        USING (work_hk)
)

SELECT * FROM base
