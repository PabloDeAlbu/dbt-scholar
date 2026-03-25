WITH base AS (
    SELECT
        sat.extract_cdk,
        sat.researchproduct_hk,
        hub.researchproduct_id,
        sat._filter_param,
        sat._filter_value,
        sat.extract_datetime,
        sat.load_datetime,
        sat.source
    FROM {{ ref('sat_openaire_researchproduct__extract') }} sat
    INNER JOIN {{ ref('hub_openaire_researchproduct') }} hub
        USING (researchproduct_hk)
)

SELECT * FROM base
