{{ config(materialized='view') }}

WITH sedici AS (
    SELECT
        EXTRACT(YEAR FROM date_issued)::int AS publication_year,
        'sedici'::text AS source_system
    FROM {{ ref('fct_unlp_ir_item_publication') }}
    WHERE date_issued IS NOT NULL
),

openalex AS (
    SELECT
        EXTRACT(YEAR FROM publication_year)::int AS publication_year,
        'openalex'::text AS source_system
    FROM {{ ref('fct_unlp_openalex_work_publication') }}
    WHERE publication_year IS NOT NULL
),

base AS (
    SELECT * FROM sedici
    UNION ALL
    SELECT * FROM openalex
)

SELECT
    publication_year,
    source_system,
    COUNT(*) AS publication_count
FROM base
GROUP BY 1, 2
