{{ config(materialized='view') }}

WITH latest_sat_authorship AS (
    SELECT *
    FROM {{ latest_satellite(ref('sat_openalex_authorship'), 'authorship_hk') }}
),

author AS (
    SELECT
        ordered_author_name.work_hk,
        STRING_AGG(
            ordered_author_name.display_name::text,
            '|' ORDER BY ordered_author_name.author_position_sort_key, ordered_author_name.display_name::text
        ) AS author
    FROM (
        SELECT DISTINCT
            brg.work_hk,
            COALESCE(
                NULLIF(dim_author.display_name, '!UNKNOWN'),
                NULLIF(latest_sat_authorship.author_display_name, '!UNKNOWN')
            ) AS display_name,
            CASE latest_sat_authorship.author_position
                WHEN 'first' THEN 1
                WHEN 'middle' THEN 2
                WHEN 'last' THEN 3
                ELSE 4
            END AS author_position_sort_key
        FROM {{ ref('brg_openalex_work_authorship') }} brg
        LEFT JOIN {{ ref('dim_openalex_author') }} dim_author USING (author_hk)
        LEFT JOIN latest_sat_authorship USING (authorship_hk)
        WHERE COALESCE(
            NULLIF(dim_author.display_name, '!UNKNOWN'),
            NULLIF(latest_sat_authorship.author_display_name, '!UNKNOWN')
        ) IS NOT NULL
    ) ordered_author_name
    GROUP BY ordered_author_name.work_hk
),

base AS (
    SELECT
        work_hk,
        {{ clean_text('work_id') }} AS work_id,
        {{ clean_text('title') }} AS title,
        publication_year,
        {{ clean_text('publication_date') }} AS publication_date,
        {{ clean_text('doi') }} AS doi,
        {{ clean_text('type') }} AS type
    FROM {{ ref('fct_unlp_openalex_work_publication') }}
),

final AS (
    SELECT
        base.work_id AS id,
        base.type,
        {{ clean_text('author.author') }} AS author,
        base.publication_date,
        CASE
            WHEN base.publication_year IS NOT NULL THEN EXTRACT(YEAR FROM base.publication_year)::int::text
        END AS publication_year,
        base.doi,
        base.title
    FROM base
    LEFT JOIN author USING (work_hk)
)

SELECT * FROM final
WHERE publication_year = '2025' OR publication_year = '2026'
