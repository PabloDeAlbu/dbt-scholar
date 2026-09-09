{{ config(materialized='view') }}

WITH base AS (
    SELECT *
    FROM {{ ref('unlp_03_cruce_sedici_scholar') }}
)

SELECT *
FROM base
WHERE estado_correspondencia <> 'Confirmada'
ORDER BY
    (anio_publicacion = 2026) DESC,
    anio_publicacion DESC NULLS LAST,
    estado_correspondencia,
    titulo_sedici
