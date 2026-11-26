-- ¿Con qué organizaciones relaciona OpenAIRE a cada researchproduct?
WITH researchproduct_organization AS (
    SELECT DISTINCT
        researchproduct_hk,
        organization_hk
    FROM {{ ref('brg_openaire_researchproduct_organization') }}
),

-- ¿Qué atributos expone cada relación entre researchproduct y organización?
final AS (
    SELECT
        researchproduct.researchproduct_id,
        organization.organization_id,
        organization.acronym,
        organization.legalname,
        researchproduct.publication_date,
        relationship.researchproduct_hk,
        relationship.organization_hk
    FROM researchproduct_organization AS relationship
    INNER JOIN {{ ref('fct_openaire_researchproduct_publication') }} AS researchproduct
        USING (researchproduct_hk)
    INNER JOIN {{ ref('dim_openaire_organization') }} AS organization
        USING (organization_hk)
)

SELECT *
FROM final
