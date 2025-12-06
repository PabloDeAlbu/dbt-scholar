{{ config(materialized = 'table') }}

{# WITH latest_sat AS {{ latest_satellite(ref('sat_oai_record'), 'record_hk') }},

base as (
    SELECT DISTINCT
        sat_i.record_id,
        sat_i.title,
        sat_i.date_issued,

        dim_col.set_id,
        dim_col.name as col_name,

        sat_i.record_hk
        
    FROM latest_sat sat_i
    {# JOIN {{ref('brg_oai_record_col')}} brg_record_col USING (record_hk) #}
    {# INNER JOIN {{ref('dim_oai_col')}} dim_col ON brg_record_col.col_hk = dim_col.set_hk #}
) #}

SELECT 1