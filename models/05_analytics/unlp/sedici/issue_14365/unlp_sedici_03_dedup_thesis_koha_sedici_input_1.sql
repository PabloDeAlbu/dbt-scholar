WITH source AS (
    SELECT
        *,
        COALESCE(
            NULLIF(BTRIM(anio_publicacion::text), ''),
            NULLIF(BTRIM(anio_titulo::text), '')
        ) AS available_year
    FROM {{ ref('tesis_fda_koha_sedici_cruce_2026') }}
)

SELECT
    'koha_fda'::text AS source,
    id_koha::text AS id,
    BTRIM(titulo) AS title,
    NULL::text AS subtitle,
    'tesis'::text AS type,
    COALESCE(
        NULLIF(BTRIM(autor), ''),
        'AUTOR_NO_INFORMADO_' || id_koha::text
    ) AS author,
    SUBSTRING(available_year FROM '[12][0-9]{3}') AS date,
    NULL::text AS doi,
    NULL::text AS isbn,
    NULL::text AS issn,
    NULL::text AS description
FROM source
