{{ config(
    materialized='table',
    indexes=[
      {'columns': ['source_name', 'type_raw'], 'unique': true},
      {'columns': ['dedup_type']}
    ],
    post_hook=["analyze {{ this }}"]
) }}

WITH seed_libros_type AS (
    SELECT DISTINCT
        'seed_libros'::text AS source_name,
        NULLIF({{ clean_text('serie') }}, '')::text AS type_raw,
        'LIBRO'::text AS dedup_type
    FROM {{ ref('libros_catedra_fda_koha_sedici_cruce_2026') }}
),

seed_tesis_type AS (
    SELECT DISTINCT
        'seed_tesis'::text AS source_name,
        NULLIF({{ clean_text('tipo_tesis') }}, '')::text AS type_raw,
        'TESIS'::text AS dedup_type
    FROM {{ ref('tesis_fda_koha_sedici_cruce_2026') }}
),

unioned AS (
    SELECT * FROM seed_libros_type
    UNION
    SELECT * FROM seed_tesis_type
)

SELECT
    source_name,
    type_raw,
    dedup_type
FROM unioned
