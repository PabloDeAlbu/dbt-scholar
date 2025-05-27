WITH base AS (
    SELECT 
        item_doi_hk,
        item_hk,
        doi_hk,
        load_datetime,
        source
    FROM {{ref('link_dspace5_item_doi')}}
)

SELECT * FROM base