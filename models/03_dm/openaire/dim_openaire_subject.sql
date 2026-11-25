{{ config(materialized = 'table') }}

-- ¿Qué términos de vocabularios temáticos controlados fueron registrados por OpenAIRE?
WITH subject AS (
    SELECT 
        subject_scheme,
        subject_value,
        subject_hk
    FROM {{ref('hub_openaire_subject')}} hub_subject
    WHERE subject_scheme != 'keyword'
),

-- ¿Qué subjects quedan disponibles para clasificar researchproducts?
final AS (
    SELECT
        subject_scheme,
        subject_value,
        subject_hk
    FROM subject
)

SELECT *
FROM final
