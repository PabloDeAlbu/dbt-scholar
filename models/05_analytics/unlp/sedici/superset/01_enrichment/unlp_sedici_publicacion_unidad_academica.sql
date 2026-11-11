{{ config(materialized='view') }}

-- ¿Qué publicaciones forman el universo analizado?
WITH publicacion AS (
    SELECT
        item_id,
        owning_root_community_title,
        owning_root_child_community_title
    FROM {{ ref('unlp_sedici_publicacion_base') }}
),

-- ¿Cuál es la comunidad raíz y cuál es la comunidad de nivel 2 de cada publicación?
ubicacion_comunidad AS (
    SELECT
        item_id,
        owning_root_community_title AS comunidad_raiz,
        owning_root_child_community_title AS comunidad_nivel_2
    FROM publicacion
),

-- ¿La comunidad de nivel 2 se encuentra bajo la raíz Unidades académicas?
evidencia_unidad_academica AS (
    SELECT
        item_id,
        comunidad_raiz,
        comunidad_nivel_2,
        COALESCE(comunidad_raiz = 'Unidades académicas', FALSE)
            AND comunidad_nivel_2 IS NOT NULL AS es_unidad_academica
    FROM ubicacion_comunidad
),

-- ¿Qué clasificación por unidad académica se presenta para cada publicación?
final AS (
    SELECT
        item_id,
        comunidad_raiz,
        comunidad_nivel_2,
        es_unidad_academica,
        CASE
            WHEN es_unidad_academica THEN comunidad_nivel_2
        END AS unidad_academica,
        CASE
            WHEN es_unidad_academica
                THEN 'Comunidad de nivel 2 bajo Unidades académicas'
            ELSE 'Sin evidencia'
        END AS unidad_academica_evidencia
    FROM evidencia_unidad_academica
)

SELECT *
FROM final
