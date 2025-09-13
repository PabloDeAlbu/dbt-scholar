with base AS (
    SELECT
        hub_rp.researchproduct_id,
        sat_rp.publicly_funded,
        sat_rp.main_title,
        sat_rp.publication_date,
        sat_rp.type,
        sat_rp.is_green,
        sat_rp.is_in_diamond_journal,
        sat_rp.language_code,
        sat_rp.language_label,
        sat_rp.best_access_right,
        sat_rp.best_access_right_uri,
        sat_rp.citation_class,
        sat_rp.citation_count,
        sat_rp.impulse,
        sat_rp.impulse_class,
        sat_rp.influence,
        sat_rp.influence_class,
        sat_rp.popularity,
        sat_rp.popularity_class,
        sat_rp.downloads,
        sat_rp.views,
        sat_rp.publisher,
        sat_rp.embargo_end_date,
        hub_rp.researchproduct_hk
    FROM {{ref('hub_openaire_researchproduct')}} hub_rp
    INNER JOIN {{ref('sat_openaire_researchproduct')}} sat_rp USING (researchproduct_hk)
)

SELECT * FROM base
