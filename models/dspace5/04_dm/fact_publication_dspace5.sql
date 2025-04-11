WITH base as (
    SELECT 
        hub_item.item_id,
        hub_handle.handle,
        hub_doi.doi,
        sat_item.title,
        sat_item.type,
        sat_item.dateavailable,
        sat_item.dateissued,
        hub_handle.handle_hk,
        hub_doi.doi_hk
    FROM {{ref('hub_dspace5_item')}} hub_item
    INNER JOIN {{ref('sat_dspace5_item')}} sat_item ON hub_item.item_hk = sat_item.item_hk
    INNER JOIN {{ref('link_dspace5_item_handle')}} link_item_handle ON link_item_handle.item_hk = hub_item.item_hk
    INNER JOIN {{ref('hub_dspace5_handle')}} hub_handle ON link_item_handle.handle_hk = hub_handle.handle_hk
    LEFT JOIN {{ref('link_dspace5_item_doi')}} link_item_doi ON link_item_doi.item_hk = hub_item.item_hk
    LEFT JOIN {{ref('hub_dspace5_doi')}} hub_doi ON link_item_doi.doi_hk = hub_doi.doi_hk
    WHERE 
    -- excluyo items que no tengan dc.date.issued
    LEFT(CAST(sat_item.dateissued AS TEXT), 4) != '9999'

)

SELECT * FROM base