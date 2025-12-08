{{ config(materialized = 'table') }}

WITH latest_sat AS {{ latest_satellite(ref('sat_oai_record'), 'record_hk') }},

base as (
    SELECT DISTINCT
        sat_i.record_id,
        sat_i.title,
        sat_i.date_issued,

        {# dim_set.set_id, #}
        {# dim_set.set_name as set_name, #}

        sat_i.record_hk
        
    FROM latest_sat sat_i
    {# JOIN {{ref('brg_oai_record_set')}} brg_record_set USING (record_hk) #}
    {# INNER JOIN {{ref('dim_oai_set')}} dim_set ON brg_record_set.set_hk = dim_set.set_hk #}
)

SELECT * FROM base
