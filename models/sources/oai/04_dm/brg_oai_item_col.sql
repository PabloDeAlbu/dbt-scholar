WITH base AS (
    SELECT 
        item_col_hk,
        item_hk,
        col_hk,
        load_datetime
    FROM {{ ref('link_oai_item_col') }} 
)

SELECT * FROM base
