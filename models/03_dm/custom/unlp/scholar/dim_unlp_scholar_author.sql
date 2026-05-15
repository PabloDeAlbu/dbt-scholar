{{ config(materialized='table') }}

WITH final AS (
    SELECT
        scholar_user_id,
        author_name,
        profile_url,
        affiliation,
        verified_email,
        verified_email_domain,
        has_unlp_verified_email,
        mentions_unlp_affiliation,
        (has_unlp_verified_email OR mentions_unlp_affiliation) AS is_unlp_profile,
        cited_by_count,
        interests_json,
        interests,
        interest_count,
        source_system,
        entity_type,
        source_file,
        load_datetime
    FROM {{ ref('ldg_unlp_scholar_author') }}
)
SELECT * FROM final
