WITH sedici AS (
    SELECT *,
        REGEXP_REPLACE(TRANSLATE(LOWER(COALESCE(titulo, '')), 'áéíóúüñ', 'aeiouun'),
            '[^a-z0-9]+', '', 'g') AS titulo_normalizado
    FROM {{ ref('unlp_autor_publicaciones_sedici') }}
    WHERE esta_publicada_visible
),

googlescholar AS (
    SELECT *,
        REGEXP_REPLACE(TRANSLATE(LOWER(COALESCE(titulo, '')), 'áéíóúüñ', 'aeiouun'),
            '[^a-z0-9]+', '', 'g') AS titulo_normalizado
    FROM {{ ref('unlp_autor_publicaciones_googlescholar') }}
),

candidato AS (
    SELECT
        sedici.persona, sedici.item_id,
        scholar.id_publicacion_scholar,
        scholar.titulo AS titulo_scholar,
        scholar.anio_publicacion AS anio_scholar,
        scholar.url_publicacion_scholar,
        scholar.id_cluster_googlescholar,
        scholar.url_cluster_googlescholar,
        CASE
            WHEN scholar.titulo_normalizado = sedici.titulo_normalizado
                THEN 'titulo_normalizado_y_anio'
            ELSE 'titulo_contenido_y_anio'
        END AS regla_correspondencia,
        ROW_NUMBER() OVER (
            PARTITION BY sedici.persona, sedici.item_id
            ORDER BY CASE WHEN scholar.titulo_normalizado = sedici.titulo_normalizado
                THEN 1 ELSE 2 END,
                scholar.posicion_en_perfil, scholar.id_publicacion_scholar
        ) AS prioridad
    FROM sedici
    INNER JOIN googlescholar AS scholar
        ON scholar.persona = sedici.persona
       AND scholar.anio_publicacion = sedici.anio_publicacion
       AND (
           scholar.titulo_normalizado = sedici.titulo_normalizado
           OR (LENGTH(scholar.titulo_normalizado) >= 20
               AND LENGTH(sedici.titulo_normalizado) >= 20
               AND (scholar.titulo_normalizado LIKE '%' || sedici.titulo_normalizado || '%'
                   OR sedici.titulo_normalizado LIKE '%' || scholar.titulo_normalizado || '%'))
       )
),

final AS (
    SELECT
        sedici.persona,
        sedici.evidencia_identidad,
        sedici.evidencia_publicacion,
        sedici.item_id,
        sedici.handle,
        sedici.url_sedici,
        sedici.titulo AS titulo_sedici,
        sedici.anio_publicacion AS anio_sedici,
        sedici.tipo_publicacion,
        sedici.subtipo_publicacion,
        sedici.coleccion,
        sedici.comunidades,
        sedici.metadato_autor,
        sedici.nombre_autor_observado,
        sedici.autoridad_autor,
        candidato.id_publicacion_scholar,
        candidato.titulo_scholar,
        candidato.anio_scholar,
        candidato.url_publicacion_scholar,
        candidato.id_cluster_googlescholar,
        candidato.url_cluster_googlescholar,
        candidato.regla_correspondencia,
        CASE
            WHEN candidato.regla_correspondencia = 'titulo_normalizado_y_anio' THEN 'alta'
            WHEN candidato.regla_correspondencia = 'titulo_contenido_y_anio' THEN 'media'
            ELSE 'sin_correspondencia'
        END AS nivel_confianza,
        candidato.id_publicacion_scholar IS NOT NULL AS vinculada_al_perfil,
        candidato.id_publicacion_scholar IS NULL AS requiere_revision,
        'https://scholar.google.com/scholar?lookup_url=' || sedici.url_sedici
            AS lookup_url_scholar,
        'https://scholar.google.com/scholar?as_q=&as_epq='
            || REPLACE(sedici.titulo, ' ', '%20') || '&as_occt=any'
            AS busqueda_titulo_scholar
    FROM sedici
    LEFT JOIN candidato
        ON candidato.persona = sedici.persona
       AND candidato.item_id = sedici.item_id
       AND candidato.prioridad = 1
)

SELECT * FROM final
