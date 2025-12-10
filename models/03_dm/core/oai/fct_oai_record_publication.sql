{{ config(materialized = 'table') }}

WITH latest_sat AS {{ latest_satellite(ref('sat_oai_record'), 'record_hk') }},

base as (
    SELECT DISTINCT
        sat_i.record_id,
        sat_i.title,
        sat_i.date_issued,
        sat_i.record_hk
        
    FROM latest_sat sat_i
)

SELECT * FROM base
