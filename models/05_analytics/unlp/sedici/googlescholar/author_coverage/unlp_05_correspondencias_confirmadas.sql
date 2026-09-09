{{ config(materialized='view') }}

WITH base AS (
    SELECT *
    FROM {{ ref('unlp_03_cruce_sedici_scholar') }}
)

SELECT *
FROM base
WHERE estado_correspondencia = 'Confirmada'
ORDER BY anio_publicacion DESC NULLS LAST, titulo_sedici
