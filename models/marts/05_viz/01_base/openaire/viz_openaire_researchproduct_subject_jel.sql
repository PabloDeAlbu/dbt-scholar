WITH base AS (
    SELECT 
        researchproduct_id,
        CASE
            WHEN dim_openaire_subject.subject_value is not null THEN TRUE 
            ELSE FALSE 
        END AS has_jel,

        dim_openaire_subject.subject_value
    FROM {{ ref('hub_openaire_researchproduct') }}
    LEFT JOIN {{ ref('brg_openaire_researchproduct_subject') }} USING (researchproduct_hk)
    LEFT JOIN {{ ref('dim_openaire_subject') }} USING (subject_hk)
    WHERE subject_scheme = 'jel' or subject_scheme is null
)

SELECT * FROM base
