{{ config(materialized='view') }}

-- ¿Qué publicaciones forman el universo base de SEDICI?
WITH publicacion AS (
    SELECT *
    FROM {{ ref('unlp_sedici_publicacion_base') }}
),

-- ¿Qué atributos directos del repositorio se presentan para su exploración?
final AS (
    SELECT
        item_id,
        handle,
        CASE
            WHEN handle IS NOT NULL
                THEN 'https://hdl.handle.net/' || handle
        END AS url_handle,
        dc_identifier_uri AS url_publicacion,
        dc_title AS titulo,
        CASE
            WHEN CHAR_LENGTH(dc_title) > 50
                THEN LEFT(dc_title, 50) || '...'
            ELSE dc_title
        END AS titulo_truncado,
        dc_type AS tipo_publicacion,
        sedici_subtype AS subtipo_publicacion,
        dc_date_issued AS fecha_publicacion,
        publication_year AS anio_publicacion,
        dc_date_accessioned AS fecha_ingreso_sedici,
        DATE_TRUNC('month', dc_date_accessioned)::date AS mes_ingreso_sedici,
        EXTRACT(YEAR FROM dc_date_accessioned)::int AS anio_ingreso_sedici,
        dc_date_available AS fecha_disponibilidad_sedici,
        DATE_TRUNC('month', dc_date_available)::date
            AS mes_disponibilidad_sedici,
        EXTRACT(YEAR FROM dc_date_available)::int
            AS anio_disponibilidad_sedici,
        owning_root_community_title AS comunidad_raiz,
        owning_root_child_community_title AS comunidad_nivel_2,
        owning_community_title AS comunidad,
        owning_collection_title AS coleccion,
        owning_community_path_titles AS ruta_en_sedici
    FROM publicacion
)

SELECT *
FROM final
