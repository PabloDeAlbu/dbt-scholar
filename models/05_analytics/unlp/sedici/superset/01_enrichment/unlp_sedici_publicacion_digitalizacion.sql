{{ config(materialized='view') }}

WITH publicacion AS (
    SELECT *
    FROM {{ ref('unlp_sedici_publicacion_base') }}
),

evidencia AS (
    SELECT
        item_id,
        BOOL_OR(
            metadatafield_fullname = 'sedici.description.note'
            AND text_value ~* 'digitaliz'
        ) AS tiene_nota_digitalizacion,
        BOOL_OR(
            metadatafield_fullname = 'dc.format.medium'
            AND text_value ~* 'digitaliz|escáner|escaner|scanner|scanned'
        ) AS tiene_formato_digitalizacion,
        STRING_AGG(DISTINCT text_value, ' | ') FILTER (
            WHERE metadatafield_fullname = 'sedici.description.note'
              AND text_value ~* 'digitaliz'
        ) AS nota_digitalizacion,
        STRING_AGG(DISTINCT text_value, ' | ') FILTER (
            WHERE metadatafield_fullname = 'dc.format.medium'
              AND text_value ~* 'digitaliz|escáner|escaner|scanner|scanned'
        ) AS formato_digitalizacion
    FROM {{ ref('int_unlp_sedicidb_item_metadatavalue') }}
    WHERE metadatafield_fullname IN (
        'sedici.description.note',
        'dc.format.medium'
    )
    GROUP BY item_id
),

evidencia_coleccion AS (
    SELECT
        collection_id,
        TRUE AS tiene_descripcion_digitalizacion,
        STRING_AGG(DISTINCT value, ' | ') AS descripcion_digitalizacion
    FROM {{ ref('int_unlp_sedicidb_collection_dc_description') }}
    WHERE value ~* 'digitaliz'
    GROUP BY collection_id
),

clasificada AS (
    SELECT
        publicacion.*,
        COALESCE(evidencia.tiene_nota_digitalizacion, FALSE)
            AS tiene_nota_digitalizacion,
        COALESCE(evidencia.tiene_formato_digitalizacion, FALSE)
            AS tiene_formato_digitalizacion,
        COALESCE(evidencia_coleccion.tiene_descripcion_digitalizacion, FALSE)
            AS tiene_descripcion_coleccion_digitalizacion,
        evidencia.nota_digitalizacion,
        evidencia.formato_digitalizacion,
        evidencia_coleccion.descripcion_digitalizacion,
        COALESCE(
            publicacion.dc_date_available IS NOT NULL
                AND publicacion.publication_year < 2000
                AND EXTRACT(YEAR FROM publicacion.dc_date_available)::int
                    - publicacion.publication_year >= 10,
            FALSE
        ) AS tiene_indicio_temporal
    FROM publicacion
    LEFT JOIN evidencia
        USING (item_id)
    LEFT JOIN evidencia_coleccion
        ON evidencia_coleccion.collection_id = publicacion.owning_collection
)

SELECT
    *,
    CASE
        WHEN tiene_nota_digitalizacion
          OR tiene_descripcion_coleccion_digitalizacion THEN 'Sí'
        WHEN tiene_formato_digitalizacion OR tiene_indicio_temporal
            THEN 'Posiblemente'
        ELSE 'No'
    END AS fue_digitalizada,
    CASE
        WHEN tiene_nota_digitalizacion THEN 'Nota del ítem'
        WHEN tiene_descripcion_coleccion_digitalizacion
            THEN 'Descripción de la colección'
        WHEN tiene_formato_digitalizacion THEN 'Formato del ítem'
        WHEN tiene_indicio_temporal THEN 'Indicio temporal'
        ELSE 'Sin evidencia'
    END AS evidencia_digitalizacion,
    NOT tiene_nota_digitalizacion
        AND NOT tiene_descripcion_coleccion_digitalizacion
        AND (tiene_formato_digitalizacion OR tiene_indicio_temporal)
        AS requiere_revision_digitalizacion,
    CASE
        WHEN tiene_nota_digitalizacion
          OR tiene_descripcion_coleccion_digitalizacion THEN NULL
        ELSE CONCAT_WS(
            '; ',
            CASE
                WHEN tiene_formato_digitalizacion
                    THEN '`dc.format.medium` menciona digitalización o escaneo'
            END,
            CASE
                WHEN tiene_indicio_temporal
                    THEN 'publicación anterior a 2000 incorporada a SEDICI al menos 10 años después'
            END
        )
    END AS motivo_revision_digitalizacion
FROM clasificada
