{{ config(materialized='table') }}

WITH latest_profile_observation AS (
    SELECT
        profile_hk,
        observed_on
    FROM {{ ref('latest_sat_googlescholar_profile') }}
)

SELECT
    relation.profile_publication_hk,
    relation.profile_hk,
    profile.scholar_user_id,
    relation.scholar_publication_id,
    publication.position,
    publication.title,
    publication.authors,
    publication.publication_details,
    publication.publication_year,
    publication.cited_by_count,
    publication.googlescholar_cluster_id,
    publication.publication_url,
    publication.cluster_url,
    publication.observed_on,
    publication.load_datetime,
    publication.source
FROM {{ ref('link_googlescholar_profile_publication') }} AS relation
INNER JOIN {{ ref('hub_googlescholar_profile') }} AS profile
    USING (profile_hk)
INNER JOIN {{ ref('latest_sat_googlescholar_profile_publication') }} AS publication
    USING (profile_publication_hk)
INNER JOIN latest_profile_observation AS profile_observation
    ON profile_observation.profile_hk = relation.profile_hk
   AND profile_observation.observed_on = publication.observed_on
