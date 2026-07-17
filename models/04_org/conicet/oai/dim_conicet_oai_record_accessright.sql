{{ config(materialized = 'table') }}

WITH base AS (
    SELECT
        dc_right
    FROM {{ ref('seed_oai_record_accessright') }}
),

final AS (
    SELECT
        base.dc_right,
        dim.access_right_label AS access_right,
        CASE
            WHEN base.dc_right = 'info:eu-repo/semantics/restrictedAccess'
                THEN 'SOLO METADATOS (REGISTRO BIBLIOGRÁFICO)'
            ELSE dim.access_right_label_es
        END AS access_right_label_es,
        dim.access_right_uri
    FROM base
    INNER JOIN {{ ref('dim_access_right') }} AS dim
        USING (dc_right)
)

SELECT * FROM final
