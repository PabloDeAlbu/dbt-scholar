-- ¿Qué publicaciones difieren de la selección FOS institucional?
WITH final AS (
    SELECT publication.record_hk
    FROM {{ ref('fct_conicet_publication') }} AS publication
    LEFT JOIN {{ ref('int_conicet_item_fos_selected') }} AS fos
        USING (record_hk)
    WHERE publication.subject_area IS DISTINCT FROM fos.fos_label_level_1
       OR publication.subject_subarea IS DISTINCT FROM fos.fos_label_level_2
       OR publication.subject_code IS DISTINCT FROM fos.fos_code_level_1
       OR publication.subject_assignment_count IS DISTINCT FROM COALESCE(fos.fos_assignment_count, 0)
       OR publication.has_multiple_subject_assignments IS DISTINCT FROM COALESCE(fos.has_multiple_fos_assignments, FALSE)
)

SELECT *
FROM final
