-- ¿Cómo se ordenan las asignaciones FOS efectivas sin separar sus atributos?
WITH ranked_assignment AS (
    SELECT
        record_hk,
        dc_subject_hk,
        uri,
        code_level_1,
        code_level_2,
        label_level_1,
        label_level_2,
        resolution_level,
        COUNT(*) OVER (PARTITION BY record_hk)::int AS fos_assignment_count,
        ROW_NUMBER() OVER (
            PARTITION BY record_hk
            ORDER BY
                (code_level_2 IS NULL),
                code_level_1 NULLS LAST,
                code_level_2 NULLS LAST,
                uri
        ) AS selection_rank
    FROM {{ ref('brg_conicet_item_fos') }}
),

-- ¿Qué asignación FOS coherente se selecciona para representar cada registro?
final AS (
    SELECT
        record_hk,
        dc_subject_hk,
        uri AS fos_uri,
        code_level_1 AS fos_code_level_1,
        code_level_2 AS fos_code_level_2,
        label_level_1 AS fos_label_level_1,
        label_level_2 AS fos_label_level_2,
        resolution_level AS fos_resolution_level,
        fos_assignment_count,
        fos_assignment_count > 1 AS has_multiple_fos_assignments
    FROM ranked_assignment
    WHERE selection_rank = 1
)

SELECT *
FROM final
