{{ config(materialized='table') }}

WITH source_data AS (
    SELECT *
    FROM {{ source('gs', 'author') }}
),

renamed AS (
    SELECT DISTINCT
        NULLIF(TRIM(source_file), '')::text AS source_file,
        NULLIF(TRIM(source_system), '')::text AS source_system,
        NULLIF(TRIM(entity_type), '')::text AS entity_type,
        NULLIF(TRIM(name), '')::text AS author_name,
        NULLIF(TRIM(profile_url), '')::text AS profile_url,
        NULLIF(TRIM("user"), '')::text AS scholar_user_id,
        NULLIF(TRIM(affiliation), '')::text AS affiliation,
        NULLIF(TRIM(verified_email), '')::text AS verified_email,
        CASE
            WHEN NULLIF(TRIM(cited_by::text), '') ~ '^[0-9]+$'
                THEN NULLIF(TRIM(cited_by::text), '')::integer
        END AS cited_by_count,
        CASE
            WHEN NULLIF(TRIM(interests), '') IS NULL THEN '[]'::jsonb
            WHEN NULLIF(TRIM(interests), '') ~ '^\[.*\]$' THEN NULLIF(TRIM(interests), '')::jsonb
            ELSE jsonb_build_array(NULLIF(TRIM(interests), ''))
        END AS interests_json,
        _load_datetime::timestamp AS load_datetime
    FROM source_data
),

enriched AS (
    SELECT
        source_file,
        source_system,
        entity_type,
        author_name,
        profile_url,
        scholar_user_id,
        affiliation,
        verified_email,
        LOWER(SUBSTRING(verified_email FROM '([A-Za-z0-9.-]+[.][A-Za-z]{2,})$')) AS verified_email_domain,
        cited_by_count,
        interests_json,
        (
            SELECT STRING_AGG(value, ' | ' ORDER BY ordinality)
            FROM jsonb_array_elements_text(interests_json) WITH ORDINALITY AS interest(value, ordinality)
        ) AS interests,
        (
            SELECT COUNT(*)
            FROM jsonb_array_elements_text(interests_json)
        )::integer AS interest_count,
        load_datetime
    FROM renamed
),

final AS (
    SELECT
        source_file,
        source_system,
        entity_type,
        author_name,
        profile_url,
        scholar_user_id,
        affiliation,
        verified_email,
        verified_email_domain,
        (verified_email_domain = 'unlp.edu.ar' OR verified_email_domain LIKE '%.unlp.edu.ar') AS has_unlp_verified_email,
        (
            COALESCE(affiliation, '') ILIKE '%universidad nacional de la plata%'
            OR COALESCE(affiliation, '') ~* '(^|[^a-z])unlp([^a-z]|$)'
        ) AS mentions_unlp_affiliation,
        cited_by_count,
        interests_json,
        interests,
        interest_count,
        load_datetime
    FROM enriched
    WHERE scholar_user_id IS NOT NULL
)

SELECT * FROM final
