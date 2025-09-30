{{ config(materialized = 'table') }}

WITH author AS (
    SELECT
        hub_author.author_id,
        hub_author.author_hk,
        sat_author.display_name,
        sat_author.works_count,
        sat_author.cited_by_count
    FROM {{ref('hub_openalex_author')}} hub_author
    INNER JOIN {{ref('sat_openalex_author')}} sat_author USING (author_hk)
),

author_orcid AS (
    SELECT
        link_author_orcid.author_hk,
        MAX(REPLACE(hub_orcid.orcid, 'https://orcid.org/', '')) AS orcid
    FROM {{ref('link_openalex_author_orcid')}} link_author_orcid
    LEFT JOIN {{ref('hub_openalex_orcid')}} hub_orcid USING (orcid_hk)
    GROUP BY link_author_orcid.author_hk
),

dim AS (
    SELECT
        author.author_id,
        author.author_hk,
        author.display_name,
        author.works_count,
        author.cited_by_count,
        COALESCE(author_orcid.orcid, '-') AS orcid,
        CASE WHEN COALESCE(author_orcid.orcid, '-') <> '-' THEN TRUE ELSE FALSE END AS has_orcid
    FROM author
    LEFT JOIN author_orcid USING (author_hk)
)

SELECT * FROM dim
