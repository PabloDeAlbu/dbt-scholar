-- ¿Qué formatos Dublin Core están asociados a cada registro OAI?
WITH record_format AS (
    SELECT
        format.dc_format,
        link.record_hk,
        link.dc_format_hk,
        link.record_format_hk
    FROM {{ ref('link_oai_record_format') }} AS link
    INNER JOIN {{ ref('hub_oai_format') }} AS format
        USING (dc_format_hk)
),

-- ¿Qué atributos expone brg_oai_record_format?
final AS (
    SELECT *
    FROM record_format
)

SELECT *
FROM final
