{{ config(materialized = 'table') }}

-- ¿Qué valores dc.type institucionales y vocabularios de origen deben mapearse?
WITH base AS (
    SELECT
        dc_type,
        CASE
            WHEN dc_type LIKE 'info:ar-repo/semantics/%' THEN 'ar-repo'
            WHEN dc_type LIKE 'info:eu-repo/semantics/%' THEN 'eu-repo'
        END AS type_vocabulary
    FROM {{ ref('seed_oai_record_type') }}
),

-- ¿A qué URI COAR se mapea cada valor institucional de dc.type?
institutional_mapping AS (
    SELECT
        base.dc_type,
        base.type_vocabulary,
        COALESCE(
            NULLIF(mapping.coar_uri, '#N/A'),
            CASE WHEN base.dc_type = 'info:eu-repo/semantics/other'
                THEN 'http://purl.org/coar/resource_type/c_1843'
            END
        ) AS resource_type_uri,
        (
            base.dc_type <> 'info:eu-repo/semantics/publishedVersion'
            AND (
                NULLIF(mapping.coar_uri, '#N/A') IS NOT NULL
                OR base.dc_type = 'info:eu-repo/semantics/other'
            )
        ) AS is_resource_type
    FROM base
    LEFT JOIN {{ ref('seed_coar_resource_types2conicet_oai_dc_types') }} AS mapping
        ON base.dc_type = mapping.record_type
),

-- ¿Qué atributos canónicos de COAR corresponden al mapeo institucional?
final AS (
    SELECT
        mapping.dc_type,
        mapping.type_vocabulary,
        resource_type.resource_type_label AS resource_type,
        mapping.resource_type_uri,
        resource_type.resource_type_label_es,
        mapping.is_resource_type
    FROM institutional_mapping AS mapping
    LEFT JOIN {{ ref('dim_resource_type') }} AS resource_type
        USING (resource_type_uri)
)

SELECT * FROM final
