WITH persona_referencia AS (
    SELECT * FROM {{ ref('seed_unlp_persona_referencia') }}
),

autor_resuelto AS (
    SELECT
        persona.nombre AS persona,
        persona.variantes_nombre,
        autor.author_id,
        autor.author_name_preferred,
        CASE
            WHEN autor.authority = persona.autoridad_voc_sedici THEN 'autoridad_voc_sedici'
            WHEN autor.orcid = persona.orcid THEN 'orcid'
            WHEN autor.scholar_user_id = persona.id_google_scholar THEN 'id_google_scholar'
        END AS evidencia_identidad,
        ROW_NUMBER() OVER (
            PARTITION BY persona.nombre
            ORDER BY CASE
                WHEN autor.authority = persona.autoridad_voc_sedici THEN 1
                WHEN autor.orcid = persona.orcid THEN 2
                WHEN autor.scholar_user_id = persona.id_google_scholar THEN 3
            END, autor.author_id
        ) AS prioridad
    FROM persona_referencia AS persona
    INNER JOIN {{ ref('dim_unlp_author') }} AS autor
        ON autor.authority = persona.autoridad_voc_sedici
        OR autor.orcid = persona.orcid
        OR autor.scholar_user_id = persona.id_google_scholar
),

variante_nombre AS (
    SELECT
        persona.nombre AS persona,
        LOWER(BTRIM(variante)) AS variante_normalizada
    FROM persona_referencia AS persona
    CROSS JOIN LATERAL REGEXP_SPLIT_TO_TABLE(
        persona.variantes_nombre, '\s*\|\s*'
    ) AS variante
),

observacion_autor AS (
    SELECT
        resuelto.persona,
        resuelto.author_id AS id_autor_resuelto,
        resuelto.author_name_preferred AS autor_resuelto,
        resuelto.evidencia_identidad,
        autoria.item_id,
        autoria.metadatafield_fullname,
        autoria.author_name_raw AS text_value,
        autoria.authority,
        autoria.author_place,
        CASE
            WHEN autoria.author_id = resuelto.author_id THEN 'identidad_resuelta'
            ELSE 'variante_nombre_sin_autoridad'
        END AS evidencia_publicacion,
        ROW_NUMBER() OVER (
            PARTITION BY resuelto.persona, autoria.item_id
            ORDER BY CASE WHEN autoria.author_id = resuelto.author_id THEN 1 ELSE 2 END,
                autoria.author_place NULLS LAST, autoria.metadata_value_id
        ) AS prioridad
    FROM autor_resuelto AS resuelto
    INNER JOIN {{ ref('brg_unlp_sedicidb_item_author') }} AS autoria
        ON autoria.author_id = resuelto.author_id
        OR (
            autoria.authority IS NULL
            AND EXISTS (
                SELECT 1 FROM variante_nombre AS variante
                WHERE variante.persona = resuelto.persona
                  AND variante.variante_normalizada = LOWER(BTRIM(autoria.author_name))
            )
        )
    WHERE resuelto.prioridad = 1
      AND autoria.author_role = 'creator_person'
),

final AS (
    SELECT
        autor.persona,
        autor.id_autor_resuelto,
        autor.autor_resuelto,
        autor.evidencia_identidad,
        autor.evidencia_publicacion,
        publicacion.item_id,
        publicacion.handle,
        publicacion.dc_identifier_uri AS url_sedici,
        publicacion.dc_title AS titulo,
        publicacion.publication_year AS anio_publicacion,
        publicacion.dc_type AS tipo_publicacion,
        publicacion.sedici_subtype AS subtipo_publicacion,
        publicacion.owning_collection_title AS coleccion,
        publicacion.owning_community_path_titles AS comunidades,
        autor.metadatafield_fullname AS metadato_autor,
        autor.text_value AS nombre_autor_observado,
        autor.authority AS autoridad_autor,
        autor.author_place AS posicion_autor,
        publicacion.in_archive AS esta_archivada,
        publicacion.withdrawn AS esta_retirada,
        publicacion.discoverable AS es_descubrible,
        (publicacion.in_archive IS TRUE AND publicacion.withdrawn IS FALSE
            AND publicacion.discoverable IS TRUE AND publicacion.handle IS NOT NULL)
            AS esta_publicada_visible
    FROM observacion_autor AS autor
    INNER JOIN {{ ref('fct_unlp_sedicidb_item_publication') }} AS publicacion
        USING (item_id)
    WHERE autor.prioridad = 1
)

SELECT * FROM final
