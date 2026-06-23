{{ config(
    materialized='table',
    indexes=[
      {'columns': ['id'], 'unique': true},
      {'columns': ['publication_year']},
      {'columns': ['type']}
    ],
    post_hook=["analyze {{ this }}"]
) }}

WITH seed_years AS (
    SELECT DISTINCT publication_year
    FROM {{ ref('fct_unlp_seed_14365_tesis_input1') }}
    WHERE publication_year IS NOT NULL
),

base AS (
    SELECT
        publication.source,
        publication.id,
        publication.publication_year,
        publication.type_raw,
        publication.title,
        publication.subtitle,
        COALESCE(type_map.dedup_type, publication.type)::text AS type,
        publication.author,
        publication.date,
        publication.doi,
        publication.isbn,
        publication.issn,
        publication.description
    FROM {{ ref('fct_unlp_sedici_dedup_publication') }} AS publication
    LEFT JOIN {{ ref('dim_unlp_sedici_14365_input_type') }} AS type_map
      ON LOWER(BTRIM(type_map.type_raw)) = LOWER(BTRIM(publication.type_raw))
    WHERE COALESCE(type_map.dedup_type, publication.type) = 'TESIS'
),

final AS (
    SELECT
        base.source,
        base.id,
        base.publication_year,
        base.type_raw,
        base.title,
        base.subtitle,
        'TESIS'::text AS type,
        base.author,
        base.date,
        base.doi,
        base.isbn,
        base.issn,
        base.description
    FROM base
    WHERE base.publication_year IN (
        SELECT publication_year
        FROM seed_years
    )
      AND base.title IS NOT NULL
      AND base.type IS NOT NULL
      AND base.author IS NOT NULL
      AND base.date IS NOT NULL
)

SELECT *
FROM final
