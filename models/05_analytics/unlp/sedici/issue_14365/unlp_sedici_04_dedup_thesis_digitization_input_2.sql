WITH source AS (
    SELECT
        "INVENTARIO"::text AS inventory_id,
        NULLIF(BTRIM("TITULO"::text), '') AS title,
        NULLIF(BTRIM("AUTOR"::text), '') AS author,
        SUBSTRING("AÑO"::text FROM '[12][0-9]{3}') AS publication_year
    FROM {{ ref('seed_issue_14365_04_thesis_fda_digitization') }}
),

grouped AS (
    SELECT
        inventory_id,
        STRING_AGG(DISTINCT title, '|' ORDER BY title) AS title,
        STRING_AGG(DISTINCT author, '|' ORDER BY author) AS author,
        MIN(publication_year) AS publication_year
    FROM source
    GROUP BY inventory_id
)

SELECT
    'fda_digitization'::text AS source,
    inventory_id AS id,
    title,
    NULL::text AS subtitle,
    'tesis'::text AS type,
    author,
    publication_year AS date,
    NULL::text AS doi,
    NULL::text AS isbn,
    NULL::text AS issn,
    NULL::text AS description
FROM grouped
