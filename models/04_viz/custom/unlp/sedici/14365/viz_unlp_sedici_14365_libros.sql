{{ config(materialized='view') }}

WITH seed_base AS (
    SELECT
        'libros_catedra_fda:' || id_koha::text AS seed_row_id,
        id_koha::text AS id_koha,
        titulo_completo::text AS seed_title,
        autor_100a::text AS seed_author,
        fecha::text AS seed_year_raw,
        titulo_completo::text AS titulo_completo,
        autor_100a::text AS autor_100a,
        serie::text AS serie,
        lugar::text AS lugar,
        editor::text AS editor,
        fecha::text AS fecha,
        NULLIF(
            BTRIM(
                REGEXP_REPLACE(
                    LOWER(REGEXP_REPLACE(titulo_completo::text, '[^[:alnum:]]+', ' ', 'g')),
                    '\s+',
                    ' ',
                    'g'
                )
            ),
            ''
        ) AS seed_title_match_key,
        NULLIF(
            BTRIM(
                REGEXP_REPLACE(
                    LOWER(
                        REGEXP_REPLACE(
                            SPLIT_PART(SPLIT_PART(titulo_completo::text, ':', 1), '/', 1),
                            '[^[:alnum:]]+',
                            ' ',
                            'g'
                        )
                    ),
                    '\s+',
                    ' ',
                    'g'
                )
            ),
            ''
        ) AS seed_title_base_match_key
    FROM {{ ref('libros_catedra_fda_koha_sedici_cruce_2026') }}
),

ir_base AS (
    SELECT
        item_hk,
        item_id,
        dc_identifier_uri AS institutional_uri,
        title AS ir_title,
        author AS ir_author,
        type AS ir_type,
        subtype AS ir_subtype,
        date_issued,
        date_accessioned,
        isbn,
        doi,
        owning_collection_title,
        owning_community_title,
        owning_root_community_title,
        NULLIF(
            BTRIM(
                REGEXP_REPLACE(
                    LOWER(REGEXP_REPLACE(title::text, '[^[:alnum:]]+', ' ', 'g')),
                    '\s+',
                    ' ',
                    'g'
                )
            ),
            ''
        ) AS ir_title_match_key,
        NULLIF(
            BTRIM(
                REGEXP_REPLACE(
                    LOWER(
                        REGEXP_REPLACE(
                            SPLIT_PART(SPLIT_PART(title::text, ':', 1), '/', 1),
                            '[^[:alnum:]]+',
                            ' ',
                            'g'
                        )
                    ),
                    '\s+',
                    ' ',
                    'g'
                )
            ),
            ''
        ) AS ir_title_base_match_key
    FROM {{ ref('fct_unlp_ir_item_publication') }}
    WHERE title IS NOT NULL
      AND LOWER(COALESCE(type, '')) LIKE '%libro%'
),

matched AS (
    SELECT
        sb.seed_row_id,
        sb.id_koha,
        sb.seed_title,
        sb.seed_author,
        sb.seed_year_raw,
        sb.titulo_completo,
        sb.autor_100a,
        sb.serie,
        sb.lugar,
        sb.editor,
        sb.fecha,
        sb.seed_title_match_key,
        sb.seed_title_base_match_key,
        ir.item_hk,
        ir.item_id,
        ir.institutional_uri,
        ir.ir_title,
        ir.ir_author,
        ir.ir_type,
        ir.ir_subtype,
        ir.date_issued,
        ir.date_accessioned,
        ir.isbn,
        ir.doi,
        ir.owning_collection_title,
        ir.owning_community_title,
        ir.owning_root_community_title,
        ir.ir_title_match_key,
        ir.ir_title_base_match_key,
        CASE
            WHEN sb.seed_title_match_key = ir.ir_title_match_key THEN 'full'
            WHEN sb.seed_title_base_match_key = ir.ir_title_base_match_key THEN 'base'
        END AS title_match_strategy,
        COUNT(ir.item_hk) OVER (PARTITION BY sb.seed_row_id) AS title_match_count,
        CASE
            WHEN ir.item_hk IS NOT NULL THEN
                ROW_NUMBER() OVER (
                    PARTITION BY sb.seed_row_id
                    ORDER BY
                        CASE
                            WHEN sb.seed_title_match_key = ir.ir_title_match_key THEN 1
                            WHEN sb.seed_title_base_match_key = ir.ir_title_base_match_key THEN 2
                            ELSE 3
                        END,
                        ir.date_issued DESC NULLS LAST,
                        ir.item_id DESC
                )
        END AS title_match_rank
    FROM seed_base AS sb
    LEFT JOIN ir_base AS ir
      ON (
            sb.seed_title_match_key = ir.ir_title_match_key
         OR sb.seed_title_base_match_key = ir.ir_title_base_match_key
      )
)

SELECT
    seed_row_id,
    id_koha,
    seed_title,
    seed_author,
    seed_year_raw,
    titulo_completo,
    autor_100a,
    serie,
    lugar,
    editor,
    fecha,
    seed_title_match_key,
    seed_title_base_match_key,
    item_hk,
    item_id,
    institutional_uri,
    ir_title,
    ir_author,
    ir_type,
    ir_subtype,
    date_issued,
    date_accessioned,
    isbn,
    doi,
    owning_collection_title,
    owning_community_title,
    owning_root_community_title,
    ir_title_match_key,
    ir_title_base_match_key,
    title_match_strategy,
    title_match_count,
    title_match_rank,
    title_match_count > 0 AS has_title_match
FROM matched
