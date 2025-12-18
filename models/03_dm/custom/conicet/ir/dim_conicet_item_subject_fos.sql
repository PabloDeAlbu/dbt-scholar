WITH 
-- 1. Traemos el Hub (filtrado por FORD)
hub_subjects AS (
    SELECT 
        dc_subject_hk,
        dc_subject as uri
    FROM {{ ref('hub_oai_subject') }}
    WHERE dc_subject like 'https://purl.org/becyt/ford/%'
),

-- 2. Preparamos el Seed para que sirva de diccionario único
-- Hacemos un UNION para tener URIs de nivel 1 y nivel 2 en la misma columna de join
dictionary_fos AS (
    -- Parte A: URIs de Nivel 2 (Hijos)
    SELECT 
        purl_level_2 as uri_key,
        fos_level_1 as label_level_1, -- Ej: "5 - Ciencias sociales"
        fos_level_2 as label_level_2, -- Ej: "5.7 - Geografía..."
        2 as hierarchy_level
    FROM {{ ref('seed_becyt_fos') }}
    WHERE purl_level_2 IS NOT NULL

    UNION DISTINCT

    -- Parte B: URIs de Nivel 1 (Padres)
    -- Generamos entradas únicas para los padres para que macheen si el Hub trae solo el padre
    SELECT 
        purl_level_1 as uri_key,
        fos_level_1 as label_level_1,
        NULL as label_level_2,        -- El padre no tiene etiqueta de hijo
        1 as hierarchy_level
    FROM {{ ref('seed_becyt_fos') }}
    WHERE purl_level_1 IS NOT NULL
),

-- 3. Cruce Final
final AS (
    SELECT 
        h.dc_subject_hk,
        h.uri,
        
        -- Extraemos el código limpio del string (para tenerlo separado por si acaso)
        SPLIT_PART(REGEXP_REPLACE(h.uri, '.*\/', ''), '.', 1) as code_level_1,
        CASE 
            WHEN h.uri ~ '.*\/\d+\.\d+$' THEN REGEXP_REPLACE(h.uri, '.*\/', '')
            ELSE NULL 
        END as code_level_2,

        -- Traemos las etiquetas bonitas del Seed
        d.label_level_1, -- Siempre tendrás la Gran Área
        d.label_level_2, -- Tendrás el Área Específica si existe
        
        -- Metadato útil: ¿A qué nivel de detalle está clasificado este paper?
        COALESCE(d.hierarchy_level, 0) as resolution_level

    FROM hub_subjects h
    -- LEFT JOIN para no perder subjects si el seed está incompleto
    LEFT JOIN dictionary_fos d ON h.uri = d.uri_key
)

SELECT * FROM final