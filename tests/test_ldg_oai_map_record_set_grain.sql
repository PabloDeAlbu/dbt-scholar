SELECT
    record_id,
    _repository_identifier,
    set_id,
    extract_datetime,
    COUNT(*) AS row_count
FROM {{ ref('ldg_oai_map_record_set') }}
GROUP BY
    record_id,
    _repository_identifier,
    set_id,
    extract_datetime
HAVING COUNT(*) > 1
