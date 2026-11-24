-- ¿Qué resolución de tipo de recurso expone la representación OAI de CONICET?
WITH
expected AS (
    SELECT
        record_hk,
        resource_type,
        resource_type_uri,
        resource_type_count,
        has_multiple_resource_types
    FROM {{ ref('fct_conicet_oai_record_publication') }}
),

-- ¿Qué publicaciones difieren de la resolución definida para OAI?
final AS (
    SELECT expected.record_hk
    FROM expected
    LEFT JOIN {{ ref('fct_conicet_publication') }} AS publication
        USING (record_hk)
    WHERE publication.resource_type IS DISTINCT FROM expected.resource_type
       OR publication.resource_type_uri IS DISTINCT FROM expected.resource_type_uri
       OR publication.resource_type_count IS DISTINCT FROM expected.resource_type_count
       OR publication.has_multiple_resource_types IS DISTINCT FROM expected.has_multiple_resource_types
)

SELECT * FROM final
