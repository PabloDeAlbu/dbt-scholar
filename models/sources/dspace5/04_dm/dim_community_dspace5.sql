{{ config(materialized = 'table') }}

WITH base AS (
    SELECT 
        lnk_com_col.collection_hk,
        lnk_com_col.community_hk,
        lnk_com_com_6.child_comm_hk as com_6_hk,
        lnk_com_com_5.child_comm_hk as com_5_hk,
        lnk_com_com_4.child_comm_hk as com_4_hk,
        lnk_com_com_3.child_comm_hk as com_3_hk,
        lnk_com_com_2.child_comm_hk as com_2_hk,
        lnk_com_com_1.child_comm_hk as com_1_hk
    FROM {{ref('link_dspace5_community_collection')}} lnk_com_col
    INNER JOIN {{ref('link_dspace5_community_community')}} lnk_com_com_6 ON
        lnk_com_col.community_hk = lnk_com_com_6.parent_comm_hk
    LEFT JOIN {{ref('link_dspace5_community_community')}} lnk_com_com_5 ON
        lnk_com_com_6.child_comm_hk = lnk_com_com_5.parent_comm_hk
    LEFT JOIN {{ref('link_dspace5_community_community')}} lnk_com_com_4 ON
        lnk_com_com_5.child_comm_hk = lnk_com_com_4.parent_comm_hk
    LEFT JOIN {{ref('link_dspace5_community_community')}} lnk_com_com_3 ON
        lnk_com_com_4.child_comm_hk = lnk_com_com_3.parent_comm_hk
    LEFT JOIN {{ref('link_dspace5_community_community')}} lnk_com_com_2 ON
        lnk_com_com_3.child_comm_hk = lnk_com_com_2.parent_comm_hk
    LEFT JOIN {{ref('link_dspace5_community_community')}} lnk_com_com_1 ON
        lnk_com_com_2.child_comm_hk = lnk_com_com_1.parent_comm_hk
),

final AS (
    SELECT 
        base.*,
        mv_com_6.text_value as com_6, 
        mv_com_5.text_value as com_5, 
        mv_com_4.text_value as com_4, 
        mv_com_3.text_value as com_3, 
        mv_com_2.text_value as com_2, 
        mv_com_1.text_value as com_1
    FROM base
    INNER JOIN {{ ref('dim_community_metadatavalue_dspace5')}} mv_com_6 ON
        base.com_6_hk = mv_com_6.community_hk AND mv_com_6.short_id = 'dc' AND mv_com_6.element = 'title' AND mv_com_6.qualifier is null
    LEFT JOIN {{ ref('dim_community_metadatavalue_dspace5')}} mv_com_5 ON
        base.com_5_hk = mv_com_5.community_hk AND mv_com_5.short_id = 'dc' AND mv_com_5.element = 'title' AND mv_com_5.qualifier is null
    LEFT JOIN {{ ref('dim_community_metadatavalue_dspace5')}} mv_com_4 ON
        base.com_4_hk = mv_com_4.community_hk AND mv_com_4.short_id = 'dc' AND mv_com_4.element = 'title' AND mv_com_4.qualifier is null
    LEFT JOIN {{ ref('dim_community_metadatavalue_dspace5')}} mv_com_3 ON
        base.com_3_hk = mv_com_3.community_hk AND mv_com_3.short_id = 'dc' AND mv_com_3.element = 'title' AND mv_com_3.qualifier is null
    LEFT JOIN {{ ref('dim_community_metadatavalue_dspace5')}} mv_com_2 ON
        base.com_2_hk = mv_com_2.community_hk AND mv_com_2.short_id = 'dc' AND mv_com_2.element = 'title' AND mv_com_2.qualifier is null
    LEFT JOIN {{ ref('dim_community_metadatavalue_dspace5')}} mv_com_1 ON
        base.com_1_hk = mv_com_1.community_hk AND mv_com_1.short_id = 'dc' AND mv_com_1.element = 'title' AND mv_com_1.qualifier is null
)

SELECT * FROM final
