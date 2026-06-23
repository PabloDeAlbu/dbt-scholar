{{ config(
    materialized='table',
    indexes=[
      {'columns': ['id'], 'unique': true},
      {'columns': ['publication_year']},
      {'columns': ['type']}
    ],
    post_hook=["analyze {{ this }}"]
) }}

WITH seed_base AS (
    SELECT
        id_koha::text AS id_koha,
        titulo::text AS title_raw,
        autor::text AS author_raw,
        tipo_tesis::text AS type_raw,
        COALESCE(anio_titulo::text, anio_publicacion::text) AS seed_year_raw
    FROM {{ ref('tesis_fda_koha_sedici_cruce_2026') }}
),

prepared AS (
    SELECT
        'koha_seed'::text AS source,
        'tesis_fda:' || id_koha AS id,
        NULLIF({{ clean_text('seed_base.type_raw') }}, '')::text AS type_raw,
        NULLIF({{ clean_text('seed_base.title_raw') }}, '')::text AS title,
        NULL::text AS subtitle,
        COALESCE(type_map.dedup_type, 'TESIS')::text AS type,
        NULLIF(
            REPLACE({{ clean_text('seed_base.author_raw') }}, '|', ' '),
            ''
        )::text AS author,
        NULLIF(SUBSTRING(seed_base.seed_year_raw FROM '(1[0-9]{3}|20[0-9]{2}|2100)'), '')::integer AS publication_year,
        NULL::text AS doi,
        NULL::text AS isbn,
        NULL::text AS issn,
        NULL::text AS description
    FROM seed_base
    LEFT JOIN {{ ref('dim_unlp_seed_14365_input_type') }} AS type_map
      ON type_map.source_name = 'seed_tesis'
     AND LOWER(COALESCE(BTRIM(type_map.type_raw), '')) = LOWER(COALESCE(BTRIM(seed_base.type_raw), ''))
),

final AS (
    SELECT
        source,
        id,
        publication_year,
        type_raw,
        title,
        subtitle,
        type,
        author,
        CASE
            WHEN publication_year IS NOT NULL THEN LPAD(publication_year::text, 4, '0')
        END::text AS date,
        doi,
        isbn,
        issn,
        description
    FROM prepared
    WHERE title IS NOT NULL
      AND author IS NOT NULL
      AND publication_year IS NOT NULL
)

SELECT *
FROM final
