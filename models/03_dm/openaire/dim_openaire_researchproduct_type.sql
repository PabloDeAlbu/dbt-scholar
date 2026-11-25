{{ config(materialized = 'table') }}

-- ¿Qué términos integran el vocabulario de tipos de researchproduct utilizado por OpenAIRE?
WITH researchproduct_type AS (
    SELECT
        type AS researchproduct_type,
        label AS researchproduct_type_label,
        vocabulary_code,
        vocabulary_url,
        term_url AS researchproduct_type_uri,
        sort_order,
        is_controlled_vocabulary
    FROM {{ ref('seed_openaire_researchproduct_type') }}
),

-- ¿Qué tipos canónicos quedan disponibles para clasificar researchproducts?
final AS (
    SELECT
        researchproduct_type,
        researchproduct_type_label,
        vocabulary_code,
        vocabulary_url,
        researchproduct_type_uri,
        sort_order,
        is_controlled_vocabulary
    FROM researchproduct_type
)

SELECT *
FROM final
