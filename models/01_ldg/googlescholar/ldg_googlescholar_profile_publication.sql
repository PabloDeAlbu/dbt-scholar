{{ config(materialized='table') }}

SELECT
    NULLIF(BTRIM(source_file), '')::text AS source_file,
    NULLIF(BTRIM(source_system), '')::text AS source_system,
    NULLIF(BTRIM(entity_type), '')::text AS entity_type,
    NULLIF(BTRIM(scholar_user_id), '')::text AS scholar_user_id,
    NULLIF(BTRIM(scholar_publication_id), '')::text AS scholar_publication_id,
    position::integer AS position,
    NULLIF(BTRIM(title), '')::text AS title,
    NULLIF(BTRIM(authors), '')::text AS authors,
    NULLIF(BTRIM(publication_details), '')::text AS publication_details,
    publication_year::integer AS publication_year,
    cited_by_count::integer AS cited_by_count,
    NULLIF(BTRIM(scholargoogle_cluster_id), '')::text AS googlescholar_cluster_id,
    NULLIF(BTRIM(publication_url), '')::text AS publication_url,
    NULLIF(BTRIM(cluster_url), '')::text AS cluster_url,
    NULLIF(BTRIM(observed_on), '')::date AS observed_on,
    NULLIF(BTRIM(_load_datetime), '')::timestamp AS load_datetime
FROM {{ source('googlescholar', 'profile_publication') }}
