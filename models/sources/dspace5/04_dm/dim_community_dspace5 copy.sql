{{ config(materialized = 'table') }}

WITH base AS (
    SELECT 
        com.community_id,
        sat_h.handle as com_base_handle,
        com.community_hk
    FROM {{ref('hub_dspace5_community')}} com 
    LEFT JOIN {{ref('link_dspace5_community_community')}} lnk_com_com ON
        lnk_com_com.child_comm_hk = com.community_hk
    INNER JOIN {{ref('link_dspace5_handle_resource')}} lnk_h_r ON
        lnk_h_r.resource_hk = com.community_hk
    INNER JOIN {{ref('sat_dspace5_handle')}} sat_h ON
        sat_h.handle_hk = lnk_h_r.handle_hk
    WHERE lnk_com_com.parent_comm_hk is null AND sat_h.resource_type_id = 4
),

com_hierarchy AS (
    SELECT 
        base.community_id,
        base.com_base_handle,
        lnk_base_com_2.child_comm_hk as child_comm_2_hk,
        lnk_base_com_3.child_comm_hk as child_comm_3_hk,
        lnk_base_com_4.child_comm_hk as child_comm_4_hk,
        lnk_base_com_5.child_comm_hk as child_comm_5_hk
    FROM base
    LEFT JOIN {{ref('link_dspace5_community_community')}} lnk_base_com_2 ON
        lnk_base_com_2.parent_comm_hk = base.community_hk
    LEFT JOIN {{ref('link_dspace5_community_community')}} lnk_base_com_3 ON
        lnk_base_com_3.parent_comm_hk = lnk_base_com_2.child_comm_hk
    LEFT JOIN {{ref('link_dspace5_community_community')}} lnk_base_com_4 ON
        lnk_base_com_4.parent_comm_hk = lnk_base_com_3.child_comm_hk
    LEFT JOIN {{ref('link_dspace5_community_community')}} lnk_base_com_5 ON
        lnk_base_com_5.parent_comm_hk = lnk_base_com_4.child_comm_hk
    LEFT JOIN {{ref('link_dspace5_community_community')}} lnk_base_com_6 ON
        lnk_base_com_6.parent_comm_hk = lnk_base_com_5.child_comm_hk
),

final AS (
    SELECT * 
    FROM com_hierarchy
    INNER JOIN dim_comm
)

SELECT * FROM final