-- ¿Qué descripciones Dublin Core están asociadas a cada registro OAI?
WITH record_description AS (
    SELECT
        description.dc_description,
        link.record_hk,
        link.dc_description_hk,
        link.record_description_hk
    FROM {{ ref('link_oai_record_description') }} AS link
    INNER JOIN {{ ref('hub_oai_description') }} AS description
        USING (dc_description_hk)
),

-- ¿Qué atributos expone brg_oai_record_description?
final AS (
    SELECT *
    FROM record_description
)

SELECT *
FROM final
