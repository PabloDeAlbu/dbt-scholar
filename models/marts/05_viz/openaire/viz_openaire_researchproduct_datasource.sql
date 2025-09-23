WITH base AS (
    SELECT 
        researchproduct_id,
        datasource_id,
        datasource_name,
        CASE
            WHEN datasource_name IS NULL THEN NULL
            WHEN char_length(datasource_name) > 40
                THEN left(datasource_name, 40) || '...'
            ELSE datasource_name
        END AS datasource_displayname,
        researchproduct_hk
    FROM {{ ref('hub_openaire_researchproduct') }}
    INNER JOIN {{ ref('brg_openaire_researchproduct_datasource') }} brg USING (researchproduct_hk)
    INNER JOIN {{ ref('dim_openaire_datasource') }} USING (datasource_hk)
)

SELECT * FROM base