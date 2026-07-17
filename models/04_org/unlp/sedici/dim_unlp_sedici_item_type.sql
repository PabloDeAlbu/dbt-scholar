WITH sedici_observed AS (
    SELECT DISTINCT
        'sedici'::text AS source,
        NULLIF({{ clean_text('type') }}, '')::text AS type,
        NULLIF({{ clean_text('subtype') }}, '')::text AS subtype
    FROM {{ ref('fct_unlp_sedici_item_publication') }}
    WHERE NULLIF({{ clean_text('type') }}, '') IS NOT NULL
),

sedici_map AS (
    SELECT
        observed.source,
        observed.type,
        observed.subtype,
        seed.coar_uri::text AS type_coar_uri,
        coar.label_es,
        coar.label,
        coar.parent_label_1
    FROM sedici_observed AS observed
    LEFT JOIN {{ ref('seed_sedici-types2coar-types') }} AS seed
      ON LOWER(BTRIM(seed.type)) = LOWER(BTRIM(observed.type))
    LEFT JOIN {{ ref('dim_coar_resource_type') }} AS coar
      ON seed.coar_uri = coar.coar_uri
),

openalex_map AS (
    SELECT
        'openalex'::text AS source,
        dim.work_type::text AS type,
        NULL::text AS subtype,
        dim.resource_type_uri::text AS type_coar_uri,
        coar.label_es,
        coar.label,
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
        type,
        subtype,
        type_coar_uri,
        CASE
            WHEN type_coar_uri IS NULL THEN NULL
            WHEN label IN ('JOURNAL ARTICLE', 'RESEARCH ARTICLE') THEN 'ARTÍCULO'
            WHEN label = 'DOCTORAL THESIS' THEN 'TESIS'
            WHEN label = 'THESIS' OR parent_label_1 = 'THESIS' THEN 'TESIS'
            ELSE label_es
        END AS type_dedup
    FROM unioned
)

SELECT *
FROM final
