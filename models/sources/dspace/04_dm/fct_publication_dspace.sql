{{ config(materialized='table') }}

WITH base AS (
    SELECT
        hub_i.item_uuid,
        sat_i.in_archive,
        sat_i.withdrawn,
        sat_i.discoverable,
        sat_i.item_hk,
        lnk_i_oc.owningcollection_hk
    FROM {{ ref('sat_dspace_item') }} sat_i
    INNER JOIN {{ref('hub_dspace_item')}} hub_i ON
        hub_i.item_hk = sat_i.item_hk
    LEFT JOIN {{ref('link_dspace_item_owningcollection')}} lnk_i_oc ON
        lnk_i_oc.item_hk = hub_i.item_hk
)

SELECT * FROM base