{{ config(materialized='table') }}

-- Latest row from sat_oai_record__extract keyed by record_hk + repository_identifier + institution_ror.
WITH latest AS {{ latest_satellite(
    ref('sat_oai_record__extract'),
    'record_hk, repository_identifier, institution_ror',
    order_column='extract_datetime, load_datetime'
) }}

SELECT
    record_hk,
    extract_cdk,
    extract_datetime,
    load_datetime,
    repository_identifier,
    institution_ror,
    source
FROM latest
