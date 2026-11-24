{{ config(materialized = 'table') }}

-- ¿Qué asignaciones FOS relaciona OAI con cada registro de CONICET?
WITH fos_assignment AS (
    SELECT DISTINCT
        subject.record_hk,
        fos.dc_subject_hk,
        fos.uri,
        fos.code_level_1,
        fos.code_level_2,
        fos.label_level_1,
        fos.label_level_2,
        fos.resolution_level
    FROM {{ ref('brg_oai_record_subject') }} AS subject
    INNER JOIN {{ ref('dim_conicet_item_subject_fos') }} AS fos
        USING (dc_subject_hk)
),

-- ¿Qué asignaciones conservan el mayor nivel de detalle disponible en cada rama FOS?
effective_assignment AS (
    SELECT assignment.*
    FROM fos_assignment AS assignment
    WHERE assignment.code_level_2 IS NOT NULL
       OR NOT EXISTS (
            SELECT 1
            FROM fos_assignment AS child
            WHERE child.record_hk = assignment.record_hk
              AND child.code_level_1 = assignment.code_level_1
              AND child.code_level_2 IS NOT NULL
       )
),

-- ¿Qué relaciones efectivas entre registros CONICET y asignaciones FOS se exponen?
final AS (
    SELECT
        record_hk,
        dc_subject_hk,
        uri,
        code_level_1,
        code_level_2,
        label_level_1,
        label_level_2,
        resolution_level
    FROM effective_assignment
)

SELECT *
FROM final
