{{ config(materialized = 'table') }}

WITH latest_sat AS {{ latest_satellite(ref('sat_oai_record'), 'record_hk') }},

base as (
    SELECT DISTINCT
        sat_i.record_id,
        sat_i.title,
        sat_i.date_issued,
        dim_record_type.label as coar_label,
        dim_record_type.coar_uri,
        sat_i.record_hk
        
    FROM latest_sat sat_i
    INNER JOIN {{ref('brg_oai_record_type')}} USING (record_hk)
    INNER JOIN {{ref('dim_oai_record_type')}} dim_record_type USING (type_hk)
)

SELECT * FROM base
