{{ config(materialized = 'table') }}

WITH latest_sat AS {{ latest_satellite(ref('sat_oai_item'), 'item_hk') }},

base as (
    SELECT DISTINCT
        sat_i.item_id,
        sat_i.title,
        sat_i.date_issued,

        dim_col.set_id,
        dim_col.name as col_name,

        sat_i.item_hk
        
    FROM latest_sat sat_i
    JOIN {{ref('brg_oai_item_col')}} brg_item_col USING (item_hk)
    INNER JOIN {{ref('dim_oai_col')}} dim_col ON brg_item_col.col_hk = dim_col.set_hk
)

SELECT * FROM base
