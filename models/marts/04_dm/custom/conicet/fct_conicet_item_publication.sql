{{ config(materialized = 'table') }}

WITH latest_sat AS {{ latest_satellite(ref('sat_oai_record'), 'record_hk') }},

base as (
    SELECT DISTINCT
        fct.record_id,
        fct.title,
        fct.date_issued,
        dim_type.label as coar_label,
        dim_type.coar_uri,
        fct.record_hk
        
    FROM {{ref('fct_oai_record_publication')}} fct
    INNER JOIN {{ref('brg_oai_record_type')}} USING (record_hk)
    INNER JOIN {{ref('dim_conicet_item_type')}} dim_type USING (type_hk)
)

SELECT * FROM base
