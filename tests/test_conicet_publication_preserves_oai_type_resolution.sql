-- ¿Qué valores de tipo documental declara cada registro OAI?
WITH record_dc_type AS (
    SELECT DISTINCT
        publication.record_hk,
        NULLIF(dc_type_value, '') AS dc_type
    FROM {{ ref('fct_conicet_oai_record_publication') }} AS publication
    CROSS JOIN LATERAL REGEXP_SPLIT_TO_TABLE(
        COALESCE(publication.dc_type, ''),
        '[|]'
    ) AS dc_type_value
    WHERE NULLIF(dc_type_value, '') IS NOT NULL
),

-- ¿Qué representación debería exponer el hecho institucional?
expected AS (
    SELECT
        record_dc_type.record_hk,
        MIN(type.publication_type) AS publication_type,
        MIN(type.publication_type_uri) AS publication_type_uri,
        COUNT(DISTINCT type.publication_type_uri) AS publication_type_count,
        (COUNT(DISTINCT type.publication_type_uri) > 1) AS has_multiple_publication_type
    FROM record_dc_type
    INNER JOIN {{ ref('dim_conicet_oai_record_type') }} AS type
        USING (dc_type)
    WHERE type.is_publication_type
    GROUP BY record_dc_type.record_hk
),

-- ¿Qué publicaciones difieren de la resolución definida para OAI?
final AS (
    SELECT expected.record_hk
    FROM expected
    LEFT JOIN {{ ref('fct_conicet_publication') }} AS publication
        USING (record_hk)
    WHERE publication.publication_type IS DISTINCT FROM expected.publication_type
       OR publication.publication_type_uri IS DISTINCT FROM expected.publication_type_uri
       OR publication.publication_type_count IS DISTINCT FROM expected.publication_type_count
       OR publication.has_multiple_publication_type IS DISTINCT FROM expected.has_multiple_publication_type
)

SELECT * FROM final
