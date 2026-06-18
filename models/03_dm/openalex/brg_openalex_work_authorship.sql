{{ config(materialized = 'table') }}

WITH base AS (
    SELECT DISTINCT
        hub_work.work_id,
        hub_work.work_hk,
        hub_author.author_id,
        hub_author.author_hk,
        hub_authorship.authorship_id,
        hub_authorship.authorship_hk,
        link_work_authorship.work_authorship_hk
    FROM {{ ref('link_openalex_work_authorship') }} link_work_authorship
    INNER JOIN {{ ref('hub_openalex_work') }} hub_work USING (work_hk)
    INNER JOIN {{ ref('hub_openalex_author') }} hub_author USING (author_hk)
    INNER JOIN {{ ref('hub_openalex_authorship') }} hub_authorship USING (authorship_hk)
)

SELECT * FROM base
