WITH cleaned AS (
    SELECT 
        hub_description.dc_description,
        -- Normalizamos el separador: doble punto ".. " -> ". " y quitamos prefijo "Fil:"
        regexp_replace(replace(hub_description.dc_description, '.. ', '. '), '^Fil:\\s*', '') AS description_no_prefix,
        lnk_r_i.record_hk,
        lnk_r_i.dc_description_hk,
        lnk_r_i.record_description_hk
    FROM {{ ref('link_oai_record_description') }} lnk_r_i
    JOIN {{ ref('hub_oai_description') }} hub_description USING (dc_description_hk)
    WHERE dc_description like 'Fil:%'
),

base AS (
    SELECT 
        dc_description,
        replace(trim(split_part(description_no_prefix, '. ', 1)),'Fil: ','') AS author,
        CASE 
            WHEN position('. ' IN description_no_prefix) > 0 
            THEN ltrim(substr(description_no_prefix, position('. ' IN description_no_prefix) + 2))
        END AS filliation,
        record_hk,
        dc_description_hk,
        record_description_hk
    FROM cleaned
),

final AS (
    SELECT
        *,
        COALESCE(length(filliation) - length(replace(filliation, ';', '')), 0) AS institution_count
    FROM base
)

SELECT * FROM final
