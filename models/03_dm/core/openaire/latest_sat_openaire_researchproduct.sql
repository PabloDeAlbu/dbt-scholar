{{ config(materialized='table') }}

-- Latest row from sat_openaire_researchproduct keyed by researchproduct_hk.
WITH latest AS {{ latest_satellite(ref('sat_openaire_researchproduct'), 'researchproduct_hk', order_column='load_datetime') }}

SELECT
    researchproduct_hk,
    publicly_funded,
    main_title,
    publication_date,
    type,
    is_green,
    is_in_diamond_journal,
    language_code,
    language_label,
    best_access_right,
    best_access_right_uri,
    citation_class,
    citation_count,
    impulse,
    impulse_class,
    influence,
    influence_class,
    popularity,
    popularity_class,
    downloads,
    views,
    publisher,
    embargo_end_date,
    load_datetime,
    source
FROM latest
