{{ config(materialized = 'table') }}

WITH openaire as (
    SELECT *
    FROM {{ref('fct_openaire_researchproduct')}}rchproduct_hk
),

join_coar AS (
    SELECT 
        openaire.*,
        coar.label_es as coar_type
    FROM openaire
    LEFT JOIN {{ref('seed_coar_openaire')}} coar ON
        openaire.type = coar.type
)

SELECT * FROM join_coar
