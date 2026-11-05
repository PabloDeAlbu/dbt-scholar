SELECT
    record_id,
    set_id,
    extract_datetime
FROM {{ ref('ldg_oai_map_record_set') }}
WHERE record_id <> '!UNKNOWN'
  AND (
      _repository_identifier IS NULL
      OR _source_key IS NULL
  )
