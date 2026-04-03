{{ config(materialized='table') }}

-- Latest row from sat_oai_record keyed by record_hk.
WITH latest AS {{ latest_satellite(ref('sat_oai_record'), 'record_hk', order_column='_load_datetime') }}

SELECT
    record_hk,
    record_id,
    title,
    date_issued,
    _context,
    _load_datetime AS load_datetime,
    source
FROM latest
