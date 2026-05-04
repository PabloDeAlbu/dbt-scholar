{{ config(materialized='table') }}

WITH
base AS (
    SELECT
        stg_work_author.work_author_hk,
        stg_work_author.work_hk,
        stg_work_author.author_hk,
        hub_work.work_id,
        NULLIF(hub_author.author_id, '!UNKNOWN') AS author_id,
        NULLIF(dim_author.display_name, '!UNKNOWN') AS canonical_display_name,
        NULLIF(stg_work_author.author_display_name, '!UNKNOWN') AS fallback_display_name,
        NULLIF(stg_work_author.author_orcid, '!UNKNOWN') AS fallback_orcid,
        NULLIF(stg_work_author.author_position, '!UNKNOWN') AS author_position
    FROM {{ ref('stg_openalex_work_author') }} stg_work_author
    INNER JOIN {{ ref('hub_openalex_work') }} hub_work USING (work_hk)
    LEFT JOIN {{ ref('hub_openalex_author') }} hub_author USING (author_hk)
    LEFT JOIN {{ ref('dim_openalex_author') }} dim_author USING (author_hk)
)

SELECT * FROM base
