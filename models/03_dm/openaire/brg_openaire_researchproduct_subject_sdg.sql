{{ config(materialized = 'table') }}

WITH base AS (
    SELECT
        dim_rp.researchproduct_hk,
        dim_rp.researchproduct_id,
        dim_subject.subject_hk,
        dim_subject.subject_value
    FROM {{ ref('brg_openaire_researchproduct_subject') }} brg_rp_subject
    INNER JOIN {{ ref('dim_openaire_researchproduct') }} dim_rp USING (researchproduct_hk)
    INNER JOIN {{ ref('dim_openaire_subject') }} dim_subject USING (subject_hk)
    WHERE dim_subject.subject_scheme = 'SDG'
)

SELECT * FROM base
