WITH base AS (
    SELECT 
        researchproduct_id,
        dim_openaire_subject.subject_value,
        CASE
            WHEN dim_openaire_subject.subject_value is not null THEN TRUE 
            ELSE FALSE 
        END AS has_fos,
        researchproduct_hk
    FROM {{ ref('hub_openaire_researchproduct') }}
    LEFT JOIN {{ ref('brg_openaire_researchproduct_subject') }} USING (researchproduct_hk)
    LEFT JOIN {{ ref('dim_openaire_subject') }} USING (subject_hk)
    WHERE subject_scheme = 'FOS' or subject_value is null
)

SELECT * FROM base
WHERE subject_value ~ '^\d{2}\s' or subject_value is null
