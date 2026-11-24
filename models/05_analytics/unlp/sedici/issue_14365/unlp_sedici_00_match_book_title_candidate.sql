{{ config(materialized='view') }}

-- ¿Qué libros entregó Koha FDA y qué claves de título permiten compararlos?
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

-- ¿Qué publicaciones del snapshot directo SEDICIDB pueden corresponder a libros por su tipo?
ir_base AS (
    SELECT
        item.item_id,
        item.dc_identifier_uri AS institutional_uri,
        publication.title AS ir_title,
        publication.author AS ir_author,
        publication.type_raw AS ir_type,
        publication.subtype AS ir_subtype,
        item.dc_date_issued AS date_issued,
        item.dc_date_accessioned AS date_accessioned,
        publication.isbn,
        publication.doi,
        item.owning_collection_title,
        item.owning_community_title,
        item.owning_root_community_title,
        NULLIF(
            BTRIM(
                REGEXP_REPLACE(
                    LOWER(REGEXP_REPLACE(publication.title::text, '[^[:alnum:]]+', ' ', 'g')),
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
                            SPLIT_PART(SPLIT_PART(publication.title::text, ':', 1), '/', 1),
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
    FROM {{ ref('fct_unlp_sedicidb_dedup_publication') }} AS publication
    INNER JOIN {{ ref('fct_unlp_sedicidb_item_publication') }} AS item
        ON item.handle = publication.id
    WHERE publication.title IS NOT NULL
      AND LOWER(COALESCE(publication.type_raw, '')) LIKE '%libro%'
),

-- ¿Qué estrategia de igualdad de título vincula cada libro con un ítem SEDICIDB?
title_match AS (
    SELECT
        sb.seed_row_id,
        ir.item_id,
        'full'::text AS title_match_strategy
    FROM seed_base AS sb
    INNER JOIN ir_base AS ir
        ON sb.seed_title_match_key = ir.ir_title_match_key

    UNION ALL

    SELECT
        sb.seed_row_id,
        ir.item_id,
        'base'::text AS title_match_strategy
    FROM seed_base AS sb
    INNER JOIN ir_base AS ir
        ON sb.seed_title_base_match_key = ir.ir_title_base_match_key
    WHERE sb.seed_title_match_key IS DISTINCT FROM ir.ir_title_match_key
),

-- ¿Qué candidatos de SEDICI comparten el título completo o base con cada libro de Koha?
candidate AS (
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
        title_match.title_match_strategy,
        COUNT(ir.item_id) OVER (PARTITION BY sb.seed_row_id) AS title_match_count,
        CASE
            WHEN ir.item_id IS NOT NULL THEN
                ROW_NUMBER() OVER (
                    PARTITION BY sb.seed_row_id
                    ORDER BY
                        CASE
                            WHEN title_match.title_match_strategy = 'full' THEN 1
                            WHEN title_match.title_match_strategy = 'base' THEN 2
                            ELSE 3
                        END,
                        ir.date_issued DESC NULLS LAST,
                        ir.item_id DESC
                )
        END AS title_match_rank
    FROM seed_base AS sb
    LEFT JOIN title_match
        USING (seed_row_id)
    LEFT JOIN ir_base AS ir
        USING (item_id)
),

-- ¿Qué evidencia de correspondencia se expone para revisar cada libro de Koha?
final AS (
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
    FROM candidate
)

SELECT * FROM final
