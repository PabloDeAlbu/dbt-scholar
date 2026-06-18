WITH sedici_map AS (
    SELECT
        'sedici'::text AS source,
        seed.type::text AS type_raw,
        seed.coar_uri::text AS type_coar_uri,
        coar.label_es AS coar_label_es,
        coar.label AS coar_label,
        coar.parent_label_1
    FROM {{ ref('seed_sedici-types2coar-types') }} AS seed
    LEFT JOIN {{ ref('dim_coar_resource_type') }} AS coar
        ON seed.coar_uri = coar.coar_uri
),

openalex_map AS (
    SELECT
        'openalex'::text AS source,
        dim.work_type::text AS type_raw,
        dim.resource_type_uri::text AS type_coar_uri,
        coar.label_es AS coar_label_es,
        coar.label AS coar_label,
        coar.parent_label_1
    FROM {{ ref('dim_openalex_work_type') }} AS dim
    LEFT JOIN {{ ref('dim_coar_resource_type') }} AS coar
        ON dim.resource_type_uri = coar.coar_uri
),

unioned AS (
    SELECT * FROM sedici_map
    UNION ALL
    SELECT * FROM openalex_map
),

final AS (
    SELECT
        source,
        type_raw,
        type_coar_uri,
        CASE
            WHEN type_coar_uri IS NULL THEN NULL
            WHEN coar_label IN ('JOURNAL ARTICLE', 'RESEARCH ARTICLE') THEN 'ARTÍCULO'
            WHEN coar_label = 'DOCTORAL THESIS' THEN 'TESIS'
            WHEN coar_label = 'THESIS' OR parent_label_1 = 'THESIS' THEN 'TESIS'
            ELSE coar_label_es
        END AS type
    FROM unioned
)

SELECT *
FROM final
