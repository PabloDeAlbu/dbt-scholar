{{ config(materialized='table') }}

WITH dc_identifier_uri AS (
    SELECT 
        bridge_i_doi.item_hk,
        mv_doi.text_value as doi,
        mv_type.text_value as type
    FROM {{ref('brg_dspace5_item_metadatavalue')}} bridge_i_doi 
    INNER JOIN {{ref('dim_dspace5_metadatavalue')}} mv_doi ON 
        mv_doi.metadatavalue_hk = bridge_i_doi.metadatavalue_hk

    INNER JOIN {{ref('brg_dspace5_item_metadatavalue')}} bridge_i_type ON
        bridge_i_doi.item_hk = bridge_i_type.item_hk
    INNER JOIN {{ref('dim_dspace5_metadatavalue')}} mv_type ON 
        mv_type.metadatavalue_hk = bridge_i_type.metadatavalue_hk

    WHERE
        bridge_i_doi.metadatafield_fullname = 'dc.identifier.uri' AND
        bridge_i_type.metadatafield_fullname = 'dc.type' AND
        mv_doi.text_value LIKE '%doi%'
),

sedici_identifier_other AS (
    SELECT 
        bridge_i_doi.item_hk,
        CONCAT('https://doi.org/10.', split_part(mv_doi.text_value, '10.', 2)) as doi,
        mv_type.text_value as type
        FROM {{ref('brg_dspace5_item_metadatavalue')}} bridge_i_doi 
        INNER JOIN {{ref('dim_dspace5_metadatavalue')}} mv_doi ON 
            mv_doi.metadatavalue_hk = bridge_i_doi.metadatavalue_hk

        INNER JOIN {{ref('brg_dspace5_item_metadatavalue')}} bridge_i_type ON
            bridge_i_doi.item_hk = bridge_i_type.item_hk
        INNER JOIN {{ref('dim_dspace5_metadatavalue')}} mv_type ON 
            mv_type.metadatavalue_hk = bridge_i_type.metadatavalue_hk
        WHERE 
            bridge_i_doi.metadatafield_fullname = 'sedici.identifier.other' AND 
            bridge_i_type.metadatafield_fullname = 'dc.type' AND
            mv_doi.text_value LIKE '%doi%'
),

union_cte AS (
    SELECT * FROM dc_identifier_uri
    UNION
    SELECT * FROM sedici_identifier_other
),

final AS (
    SELECT doi, type
    FROM union_cte
    WHERE doi IS NOT NULL
    ORDER BY RANDOM()
    LIMIT 3000
)

SELECT 
    final.doi,
    seed.label_es as type
FROM final
INNER JOIN {{ref('seed_sedici-types2coar-types')}} seed ON
seed.type = final.type 

{# SELECT type, count(*)
FROM union_cte
WHERE doi IS NOT NULL
GROUP BY type #}