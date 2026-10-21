SELECT
    _repository_identifier,
    set_id,
    extract_datetime,
    COUNT(*) AS row_count
FROM {{ ref('ldg_oai_sets') }}
GROUP BY
    _repository_identifier,
    set_id,
    extract_datetime
HAVING COUNT(*) > 1
