{{ config(materialized = 'table') }}

WITH latest_sat AS {{ latest_satellite(ref('sat_oai_record'), 'record_hk') }},

base as (
    SELECT DISTINCT
        sat_i.record_id,
        sat_i.title,
        sat_i.date_issued,
        sat_i.record_hk,
        coalesce(
            sat_i.date_issued >= DATE '1900-01-01'
            AND sat_i.date_issued < date_trunc('year', current_date) + interval '1 year',
            false
        ) AS valid_date_issued
    FROM latest_sat sat_i
)

SELECT * FROM base
