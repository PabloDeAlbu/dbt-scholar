{{ config(materialized = 'table') }}

WITH base as (
    SELECT 
        hub_researchproduct.researchproduct_id,
        sat_researchproduct.publicly_funded,
        sat_researchproduct.main_title,
        sat_researchproduct.publication_date,
        sat_researchproduct.is_green,
        sat_researchproduct.is_in_diamond_journal,
        sat_researchproduct.language_code,
        sat_researchproduct.language_label,
        sat_researchproduct.best_access_right,
        sat_researchproduct.best_access_right_uri,
        sat_researchproduct.citation_class,
        sat_researchproduct.citation_count,
        sat_researchproduct.impulse,
        sat_researchproduct.impulse_class,
        sat_researchproduct.influence,
        sat_researchproduct.influence_class,
        sat_researchproduct.popularity,
        sat_researchproduct.popularity_class,
        sat_researchproduct.downloads,
        sat_researchproduct.views,
        sat_researchproduct.publisher,
        sat_researchproduct.embargo_end_date,
        hub_researchproduct.researchproduct_hk
    FROM {{ref('hub_openaire_researchproduct')}} hub_researchproduct
    INNER JOIN {{ref('latest_sat_openaire_researchproduct')}} sat_researchproduct ON hub_researchproduct.researchproduct_hk = sat_researchproduct.researchproduct_hk
)

SELECT * FROM base