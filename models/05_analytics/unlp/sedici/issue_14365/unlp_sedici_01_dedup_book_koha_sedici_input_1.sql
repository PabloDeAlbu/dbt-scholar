SELECT
    'koha_fda'::text AS source,
    id_koha::text AS id,
    BTRIM(titulo_completo) AS title,
    NULL::text AS subtitle,
    'book'::text AS type,
    COALESCE(
        NULLIF(BTRIM(autor_100a), ''),
        'AUTOR_NO_INFORMADO_' || id_koha::text
    ) AS author,
    REGEXP_REPLACE(BTRIM(fecha::text), '[.]$', '') AS date,
    NULL::text AS doi,
    NULL::text AS isbn,
    NULL::text AS issn,
    NULL::text AS description
FROM {{ ref('libros_catedra_fda_koha_sedici_cruce_2026') }}
