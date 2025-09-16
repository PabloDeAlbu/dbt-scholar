WITH base AS 
    (
        SELECT *
        FROM {{ref('fct_openaire_researchproduct_publication')}}
),

url AS (
    SELECT *
    FROM {{ref('dim_openaire_url')}} 
    WHERE url like 'http://hdl.handle.net/%'
),

final AS (
    SELECT base.researchproduct_id as id, url.url, base.researchproduct_hk
    FROM {{ref('brg_openaire_researchproduct_url')}} brg
    INNER JOIN base ON 
        brg.researchproduct_hk = base.researchproduct_hk
    INNER JOIN url ON 
        brg.url_hk = url.url_hk
)

SELECT * FROM final