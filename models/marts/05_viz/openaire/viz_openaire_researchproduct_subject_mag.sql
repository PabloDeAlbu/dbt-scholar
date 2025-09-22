WITH base AS (
    SELECT 
        researchproduct_id,
        dim_openaire_subject.subject_value
    FROM {{ ref('hub_openaire_researchproduct') }}
    LEFT JOIN {{ ref('brg_openaire_researchproduct_subject') }} USING (researchproduct_hk)
    LEFT JOIN {{ ref('dim_openaire_subject') }} USING (subject_hk)
    WHERE subject_scheme = 'MAG' or subject_scheme is null
)

SELECT * FROM base
