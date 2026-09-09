{{ config(materialized='table') }}

SELECT
    NULLIF(BTRIM(source_file), '')::text AS source_file,
    NULLIF(BTRIM(source_system), '')::text AS source_system,
    NULLIF(BTRIM(entity_type), '')::text AS entity_type,
    NULLIF(BTRIM(scholar_user_id), '')::text AS scholar_user_id,
    NULLIF(BTRIM(coauthor_scholar_user_id), '')::text AS coauthor_scholar_user_id,
    position::integer AS position,
    NULLIF(BTRIM(coauthor_name), '')::text AS coauthor_name,
    NULLIF(BTRIM(coauthor_profile_url), '')::text AS coauthor_profile_url,
    NULLIF(BTRIM(coauthor_affiliation), '')::text AS coauthor_affiliation,
    NULLIF(BTRIM(coauthor_verified_email), '')::text AS coauthor_verified_email,
    NULLIF(BTRIM(observed_on), '')::date AS observed_on,
    NULLIF(BTRIM(_load_datetime), '')::timestamp AS load_datetime
FROM {{ source('googlescholar', 'profile_coauthor') }}
