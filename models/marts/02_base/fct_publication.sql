{{ config(materialized = 'table') }}

WITH base as (
    SELECT *
    FROM {{ref('hub_openaire_researchproduct')}} hub_rp
    INNER JOIN {{ref('latest_sat_openaire_researchproduct')}} sat_rp ON 
        hub_rp.researchproduct_hk = sat_rp.researchproduct_hk
),

add_pid AS (
    SELECT 
        dim.value as doi,
    	fct.researchproduct_id,
        fct.main_title,
        fct.type,
        fct.publication_date,
        fct.is_green,
        fct.is_in_diamond_journal,
        fct.language_code,
        fct.language_label,
        fct.best_access_right,
        fct.best_access_right_uri,
        fct.citation_class,
        fct.citation_count,
        fct.impulse,
        fct.impulse_class, 
        fct.influence,
        fct.influence_class,
        fct.popularity,
        fct.popularity_class,
        fct.downloads,
        fct.views,
        fct.publisher,
        fct.embargo_end_date,
    	fct.researchproduct_hk
    FROM base fct
    LEFT JOIN {{ref('brg_openaire_researchproduct_pid')}} brg ON
        fct.researchproduct_hk = brg.researchproduct_hk
    LEFT JOIN {{ref('dim_openaire_pid')}} dim ON
        brg.pid_hk = dim.pid_hk
    WHERE dim.scheme = 'doi' OR dim.scheme is null
),

join_coar AS (
    SELECT 
        add_pid.*,
        coar.label_es as coar_type
    FROM add_pid
    LEFT JOIN {{ref('seed_coar_openaire')}} coar ON
        add_pid.type = coar.type
)

SELECT * FROM join_coar
