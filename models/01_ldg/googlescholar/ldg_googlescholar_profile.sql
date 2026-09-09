{{ config(materialized='table') }}

SELECT
    NULLIF(BTRIM(source_file), '')::text AS source_file,
    NULLIF(BTRIM(source_system), '')::text AS source_system,
    NULLIF(BTRIM(entity_type), '')::text AS entity_type,
    NULLIF(BTRIM(scholar_user_id), '')::text AS scholar_user_id,
    NULLIF(BTRIM(profile_name), '')::text AS profile_name,
    NULLIF(BTRIM(profile_url), '')::text AS profile_url,
    NULLIF(BTRIM(orcid), '')::text AS orcid,
    NULLIF(BTRIM(affiliation), '')::text AS affiliation,
    NULLIF(BTRIM(organization_name), '')::text AS organization_name,
    NULLIF(BTRIM(organization_url), '')::text AS organization_url,
    NULLIF(BTRIM(verified_email), '')::text AS verified_email,
    NULLIF(BTRIM(interests), '')::text AS interests,
    NULLIF(BTRIM(observed_on), '')::date AS observed_on,
    NULLIF(BTRIM(_load_datetime), '')::timestamp AS load_datetime,
    metrics_since_year::integer AS metrics_since_year,
    cited_by_count_total::integer AS cited_by_count_total,
    cited_by_count_since::integer AS cited_by_count_since,
    h_index_total::integer AS h_index_total,
    h_index_since::integer AS h_index_since,
    i10_index_total::integer AS i10_index_total,
    i10_index_since::integer AS i10_index_since
FROM {{ source('googlescholar', 'profile') }}
