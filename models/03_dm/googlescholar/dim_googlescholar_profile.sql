{{ config(materialized='table') }}

SELECT
    profile.profile_hk,
    profile.scholar_user_id,
    attributes.profile_name,
    attributes.profile_url,
    attributes.orcid,
    attributes.affiliation,
    attributes.organization_name,
    attributes.organization_url,
    attributes.verified_email,
    attributes.interests,
    attributes.metrics_since_year,
    attributes.cited_by_count_total,
    attributes.cited_by_count_since,
    attributes.h_index_total,
    attributes.h_index_since,
    attributes.i10_index_total,
    attributes.i10_index_since,
    attributes.observed_on,
    attributes.load_datetime,
    attributes.source
FROM {{ ref('hub_googlescholar_profile') }} AS profile
INNER JOIN {{ ref('latest_sat_googlescholar_profile') }} AS attributes
    USING (profile_hk)
