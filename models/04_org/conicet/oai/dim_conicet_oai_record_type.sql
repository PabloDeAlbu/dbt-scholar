{{ config(materialized = 'table') }}

WITH base AS (
    SELECT
        dc_type,
        CASE
            WHEN dc_type LIKE 'info:ar-repo/semantics/%' THEN 'ar-repo'
            WHEN dc_type LIKE 'info:eu-repo/semantics/%' THEN 'eu-repo'
        END AS type_vocabulary
    FROM {{ ref('seed_oai_record_type') }}
),

final AS (
    SELECT
        base.dc_type,
        base.type_vocabulary,
        COALESCE(
            mapping.label,
            CASE WHEN base.dc_type = 'info:eu-repo/semantics/other' THEN 'OTHER' END
        ) AS publication_type,
        COALESCE(
            NULLIF(mapping.coar_uri, '#N/A'),
            CASE WHEN base.dc_type = 'info:eu-repo/semantics/other'
                THEN 'http://purl.org/coar/resource_type/c_1843'
            END
        ) AS publication_type_uri,
        COALESCE(
            mapping.label_es,
            CASE WHEN base.dc_type = 'info:eu-repo/semantics/other' THEN 'OTROS' END
        ) AS publication_type_label_es,
        CASE base.type_vocabulary
            WHEN 'ar-repo' THEN 10
            WHEN 'eu-repo' THEN 20
        END AS resolution_priority,
        (
            base.dc_type <> 'info:eu-repo/semantics/publishedVersion'
            AND (
                NULLIF(mapping.coar_uri, '#N/A') IS NOT NULL
                OR base.dc_type = 'info:eu-repo/semantics/other'
            )
        ) AS is_publication_type
    FROM base
    LEFT JOIN {{ ref('seed_coar_resource_types2conicet_oai_dc_types') }} AS mapping
        ON base.dc_type = mapping.record_type
)

SELECT * FROM final
