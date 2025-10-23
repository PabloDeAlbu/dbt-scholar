{{ config(materialized = 'table') }}

WITH latest_sat AS {{ latest_satellite(ref('sat_openaire_researchproduct'), 'researchproduct_hk') }},

base as (
    SELECT DISTINCT
        dim_rp.researchproduct_id,

        sat_rp.publication_date,
        sat_rp.embargo_end_date,
        sat_rp.type,
        sat_rp.main_title,
        sat_rp.language_code,
        sat_rp.language_label,
        sat_rp.publisher,

        {# dim_doi.value as doi, #}

        sat_rp.best_access_right,
        sat_rp.best_access_right_uri,
        sat_rp.publicly_funded,
        sat_rp.is_green,
        sat_rp.is_in_diamond_journal,

        COALESCE(sat_rp.downloads, 0) as downloads,
        COALESCE(sat_rp.views, 0) as views,
        sat_rp.citation_class,
        sat_rp.citation_count,
        sat_rp.impulse,
        sat_rp.impulse_class,
        sat_rp.influence,
        sat_rp.influence_class,
        sat_rp.popularity,
        sat_rp.popularity_class,

        CASE WHEN sat_rp.publication_date IS NOT NULL THEN TRUE ELSE FALSE END AS has_publication_date,

        dim_rp.researchproduct_hk
    FROM {{ref('dim_openaire_researchproduct')}} dim_rp
    INNER JOIN latest_sat sat_rp USING (researchproduct_hk)
    {# LEFT JOIN {{ref('brg_openaire_researchproduct_pid')}} brg USING(researchproduct_hk)
    LEFT JOIN {{ref('dim_openaire_pid')}} dim_doi USING (pid_hk)
    WHERE dim_doi.scheme = 'doi' OR dim_doi.scheme is null #}
)

SELECT * FROM base
