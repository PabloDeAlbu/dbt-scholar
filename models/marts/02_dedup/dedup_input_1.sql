WITH openaire as (
    SELECT 
        rp.researchproduct_id as id,
        rp.doi,
        {# author, #}
        rp.publication_date as date,
        rp.main_title as title,
        rp.type

    FROM {{ref('fct_openaire_researchproduct_publication')}} rp
    INNER JOIN {{ref('sat_openaire_researchproduct_author')}} sat_rp_a USING (researchproduct_hk)
)

SELECT * FROM openaire

