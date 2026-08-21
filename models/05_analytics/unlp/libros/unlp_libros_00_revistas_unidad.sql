{{ config(materialized='view') }}

WITH journal AS (
    SELECT
        journal_id,
        MAX(journal_title) AS journal_title,
        COUNT(*)::bigint AS published_item_count
    FROM {{ ref('fct_libros_unlp_journal_item') }}
    GROUP BY journal_id
),

item_unlp_unit AS (
    SELECT DISTINCT
        item.journal_id,
        item.item_id,
        institution.unidad_principal_unlp_node_id AS unlp_unit_id,
        institution.unidad_principal_unlp_nombre AS unlp_unit_name
    FROM {{ ref('fct_libros_unlp_journal_item') }} AS item
    INNER JOIN {{ ref('int_unlp_sedicidb_item_mods_origin_info_place') }} AS origin
        USING (item_id)
    INNER JOIN {{ ref('dim_vocsedici_institucion') }} AS institution
        ON institution.institution_node_id = origin.voc_institution_node_id
    WHERE institution.pertenece_arbol_unlp IS TRUE
      AND institution.nivel_arbol_unlp > 0
),

unit_stats AS (
    SELECT
        journal_id,
        unlp_unit_id,
        MAX(unlp_unit_name) AS unlp_unit_name,
        COUNT(DISTINCT item_id)::bigint AS unlp_unit_item_count
    FROM item_unlp_unit
    GROUP BY journal_id, unlp_unit_id
),

unit_ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY journal_id
            ORDER BY unlp_unit_item_count DESC, unlp_unit_id
        ) AS unit_rank
    FROM unit_stats
),

final AS (
    SELECT
        journal.journal_id,
        journal.journal_title,
        unit.unlp_unit_id,
        unit.unlp_unit_name,
        COALESCE(unit.unlp_unit_item_count, 0)::bigint
            AS unlp_unit_item_count,
        journal.published_item_count,
        ROUND(
            100.0 * COALESCE(unit.unlp_unit_item_count, 0)
            / journal.published_item_count,
            2
        ) AS unlp_unit_coverage_pct
    FROM journal
    LEFT JOIN unit_ranked AS unit
        ON unit.journal_id = journal.journal_id
       AND unit.unit_rank = 1
)

SELECT
    *,
    CASE
        WHEN unlp_unit_id IS NULL THEN 'not_identified'
        WHEN unlp_unit_coverage_pct >= 75 THEN 'high_confidence'
        WHEN unlp_unit_coverage_pct >= 50 THEN 'review'
        ELSE 'ambiguous'
    END AS unlp_unit_attribution_status
FROM final
