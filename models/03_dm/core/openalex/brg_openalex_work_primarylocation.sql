{{ config(materialized = 'table') }}

WITH latest_link AS (
    SELECT
        work_hk,
        source_hk
    FROM (
        SELECT
            work_hk,
            source_hk,
            ROW_NUMBER() OVER (
                PARTITION BY work_hk
                ORDER BY load_datetime DESC, source_hk
            ) AS rn
        FROM {{ ref('link_openalex_work_primarylocation_source') }}
    ) ranked
    WHERE rn = 1
),

latest_location AS (
    SELECT
        work_hk,
        source_hk,
        source_display_name,
        source_issn_l,
        license,
        license_id
    FROM (
        SELECT
            work_hk,
            source_hk,
            source_display_name,
            source_issn_l,
            license,
            license_id,
            ROW_NUMBER() OVER (
                PARTITION BY work_hk, source_hk
                ORDER BY load_datetime DESC
            ) AS rn
        FROM {{ ref('stg_openalex_work_locations') }}
    ) ranked
    WHERE rn = 1
),

base AS (
    SELECT DISTINCT
        hub_work.work_id,
        hub_work.work_hk,
        hub_source.source_id,
        hub_source.source_hk,
        NULLIF(location.source_display_name, '!UNKNOWN') AS source_display_name,
        NULLIF(location.source_issn_l, '!UNKNOWN') AS source_issn_l,
        NULLIF(location.license, '!UNKNOWN') AS license,
        NULLIF(location.license_id, '!UNKNOWN') AS license_id
    FROM latest_link link
    INNER JOIN {{ref('hub_openalex_work')}} hub_work USING (work_hk)
    INNER JOIN {{ ref('hub_openalex_source') }} hub_source USING (source_hk)
    LEFT JOIN latest_location location USING (work_hk, source_hk)
)

SELECT * FROM base
