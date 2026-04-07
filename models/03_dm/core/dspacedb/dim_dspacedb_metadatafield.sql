{{ config(materialized='table') }}

-- FIXME: esta dimensión depende hoy de modelos `stg` de metadatafieldregistry y
-- metadataschemaregistry porque `dspacedb` todavía no expone un camino DV completo
-- para esas entidades.
WITH base AS (
    SELECT
        stg_mf.metadatafield_bk,
        stg_mf.institution_ror,
        stg_mf.source_label,
        stg_ms.short_id,
        stg_mf.metadata_field_id,
        stg_mf.metadata_schema_id,
        stg_mf.element,
        stg_mf.qualifier,
        CASE
            WHEN stg_mf.qualifier IS NOT NULL
                THEN stg_ms.short_id || '.' || stg_mf.element || '.' || stg_mf.qualifier
            ELSE stg_ms.short_id || '.' || stg_mf.element
        END AS metadatafield_fullname,
        stg_mf.metadatafield_hk
    FROM {{ ref('stg_dspacedb_metadatafieldregistry') }} AS stg_mf
    JOIN {{ ref('stg_dspacedb_metadataschemaregistry') }} AS stg_ms
        USING (metadataschema_hk)
),
final AS (
    SELECT
        metadatafield_fullname,
        source_label,
        institution_ror,
        short_id,
        metadata_field_id,
        metadata_schema_id,
        element,
        qualifier,
        metadatafield_hk
    FROM base
    GROUP BY 1,2,3,4,5,6,7,8,9
)

SELECT * FROM final
