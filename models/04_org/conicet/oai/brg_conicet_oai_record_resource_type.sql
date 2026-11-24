{{ config(materialized = 'table') }}

{%- set conicet_repository_identifier = var('oai_repository_identifier') -%}
{%- set conicet_institution_ror = var('oai_institution_ror') -%}

-- ¿Qué records pertenecen al corpus OAI institucional de CONICET?
WITH conicet_record AS (
    SELECT record_hk
    FROM {{ ref('fct_oai_record_publication') }}
    WHERE repository_identifier = '{{ conicet_repository_identifier }}'
      AND institution_ror = '{{ conicet_institution_ror }}'
),

-- ¿Qué valores dc.type de cada record poseen correspondencia con un tipo de recurso COAR?
mapped_type AS (
    SELECT
        bridge.record_hk,
        type.dc_type,
        type.type_vocabulary,
        type.resource_type,
        type.resource_type_uri,
        type.resource_type_label_es
    FROM {{ ref('brg_oai_record_type') }} AS bridge
    INNER JOIN conicet_record
        USING (record_hk)
    INNER JOIN {{ ref('dim_conicet_oai_record_type') }} AS type
        ON bridge.dc_type = type.dc_type
    WHERE type.is_resource_type
),

-- ¿Qué relaciones canónicas entre records OAI y tipos de recurso COAR se conservan?
final AS (
    SELECT
        record_hk,
        resource_type_uri,
        resource_type,
        resource_type_label_es,
        STRING_AGG(DISTINCT type_vocabulary, '|' ORDER BY type_vocabulary)
            AS type_vocabularies,
        STRING_AGG(DISTINCT dc_type, '|' ORDER BY dc_type)
            AS source_dc_types
    FROM mapped_type
    GROUP BY
        record_hk,
        resource_type_uri,
        resource_type,
        resource_type_label_es
)

SELECT *
FROM final
