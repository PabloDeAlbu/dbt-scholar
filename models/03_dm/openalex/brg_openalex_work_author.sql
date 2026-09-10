{{ config(materialized = 'table') }}

WITH work_author AS (
    SELECT DISTINCT
        hub_work.work_id,
        hub_work.work_hk,
        NULLIF(hub_author.author_id, '!UNKNOWN') AS author_id,
        hub_author.author_hk,
        link_work_author.work_author_hk
    FROM {{ ref('link_openalex_work_author') }} AS link_work_author
    INNER JOIN {{ ref('hub_openalex_work') }} AS hub_work USING (work_hk)
    INNER JOIN {{ ref('hub_openalex_author') }} AS hub_author USING (author_hk)
),

work_author_observation AS (
    SELECT
        work_author_hk,
        NULLIF(author_display_name, '!UNKNOWN') AS author_display_name,
        NULLIF(author_orcid, '!UNKNOWN') AS author_orcid,
        NULLIF(author_position, '!UNKNOWN') AS author_position
    FROM {{ latest_satellite(ref('sat_openalex_work_author'), 'work_author_hk') }}
),

base AS (
    SELECT
        work_author.work_author_hk,
        work_author.work_hk,
        work_author.author_hk,
        work_author.work_id,
        work_author.author_id,
        NULLIF(dim_author.display_name, '!UNKNOWN') AS canonical_display_name,
        observation.author_display_name AS fallback_display_name,
        observation.author_orcid AS fallback_orcid,
        observation.author_position
    FROM work_author
    LEFT JOIN work_author_observation AS observation
        USING (work_author_hk)
    LEFT JOIN {{ ref('dim_openalex_author') }} AS dim_author
        USING (author_hk)
)

SELECT * FROM base
