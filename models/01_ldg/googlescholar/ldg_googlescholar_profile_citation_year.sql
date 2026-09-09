{{ config(materialized='table') }}

SELECT
    NULLIF(BTRIM(source_file), '')::text AS source_file,
    NULLIF(BTRIM(source_system), '')::text AS source_system,
    NULLIF(BTRIM(entity_type), '')::text AS entity_type,
    NULLIF(BTRIM(scholar_user_id), '')::text AS scholar_user_id,
    year::integer AS year,
    cited_by_count::integer AS cited_by_count,
    NULLIF(BTRIM(observed_on), '')::date AS observed_on,
    NULLIF(BTRIM(_load_datetime), '')::timestamp AS load_datetime
FROM {{ source('googlescholar', 'profile_citation_year') }}
