WITH base AS (
    SELECT 
        hub_type.dc_right,
        coar.coar_uri,
        coar.coar_label,
        coar.coar_label_es,
        hub_type.dc_right_hk
    FROM {{ ref('hub_oai_right') }} hub_type
    JOIN {{ref('seed_coar_access_right')}} coar USING (dc_right)
    WHERE dc_right like 'info:eu-repo/semantics/%'
)

SELECT * FROM base
