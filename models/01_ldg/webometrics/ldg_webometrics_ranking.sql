{{ config(materialized='table') }}

SELECT
    NULLIF(BTRIM(university_name), '')::text AS university_name,
    NULLIF(BTRIM(university_url), '')::text AS university_url,
    NULLIF(BTRIM(university_slug), '')::text AS university_slug,
    LOWER(NULLIF(BTRIM(country_code), ''))::text AS country_code,
    regional_rank::integer AS regional_rank,
    world_rank::integer AS world_rank,
    impact_rank::integer AS impact_rank,
    openness_rank::integer AS openness_rank,
    excellence_rank::integer AS excellence_rank,
    NULLIF(BTRIM(ranking_path), '')::text AS ranking_path,
    ranking_year::integer AS ranking_year,
    LOWER(NULLIF(BTRIM(ranking_edition), ''))::text AS ranking_edition,
    page_number::integer AS page_number,
    NULLIF(BTRIM(source_url), '')::text AS source_url,
    extract_datetime::timestamp AS extract_datetime,
    expected_page_count::integer AS expected_page_count,
    retrieved_page_count::integer AS retrieved_page_count,
    expected_university_count::integer AS expected_university_count,
    retrieved_university_count::integer AS retrieved_university_count,
    is_complete_extraction::boolean AS is_complete_extraction,
    NULLIF(BTRIM(source_partition), '')::text AS source_partition,
    NULLIF(BTRIM(_source_system), '')::text AS source_system,
    _load_datetime::timestamp AS load_datetime
FROM {{ source('webometrics', 'university_latinamerica_ranking') }}
