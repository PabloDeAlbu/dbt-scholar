-- ¿Qué asignaciones padre permanecen aunque exista una subárea de la misma rama?
WITH final AS (
    SELECT parent.record_hk, parent.dc_subject_hk
    FROM {{ ref('brg_conicet_item_fos') }} AS parent
    INNER JOIN {{ ref('brg_conicet_item_fos') }} AS child
        ON child.record_hk = parent.record_hk
       AND child.code_level_1 = parent.code_level_1
       AND child.code_level_2 IS NOT NULL
    WHERE parent.code_level_2 IS NULL
)

SELECT *
FROM final
