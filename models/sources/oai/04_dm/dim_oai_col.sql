WITH base AS (
    SELECT 
        dim_set.set_id,
        dim_set.name,
        CASE WHEN COALESCE(hub_col.col_id, '-') <> '-' THEN TRUE ELSE FALSE END AS proceseed,
        dim_set.set_hk
    FROM {{ ref('dim_oai_set') }} dim_set
    LEFT JOIN {{ ref('hub_oai_col')}} hub_col ON hub_col.col_hk = dim_set.set_hk
    WHERE set_id like 'col_%'
)

SELECT * FROM base
