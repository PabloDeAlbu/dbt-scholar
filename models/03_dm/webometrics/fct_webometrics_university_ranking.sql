{{ config(materialized='table') }}

SELECT
    university_name,
    university_url,
    university_slug,
    country_code,
    ranking_path,
    ranking_year,
    ranking_edition,
    regional_rank,
    world_rank,
    impact_rank,
    openness_rank,
    excellence_rank,
    source_url,
    source_partition,
    extract_datetime,
    load_datetime
FROM {{ ref('ldg_webometrics_ranking') }}
WHERE is_complete_extraction IS TRUE
