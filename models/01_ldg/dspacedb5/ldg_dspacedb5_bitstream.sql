WITH source AS (
  SELECT * FROM {{ source('dspacedb5', 'bitstream') }}
),
context AS (
  SELECT * FROM {{ ref('ldg_dspacedb5_context') }}
),
renamed AS (
  SELECT
    bitstream_id,
    bitstream_format_id,
    size_bytes,
    checksum,
    checksum_algorithm,
    internal_id,
    deleted,
    store_number,
    sequence_id,
    context.source_label AS _source_label,
    context.institution_ror AS _institution_ror,
    context.extract_datetime AS _extract_datetime,
    context.load_datetime AS _load_datetime
  FROM source
  CROSS JOIN context
),
ghost_record AS (
  SELECT
    -1 AS bitstream_id,
    -1 AS bitstream_format_id,
    -1 AS size_bytes,
    '!UNKNOWN' AS checksum,
    '!UNKNOWN' AS checksum_algorithm,
    '!UNKNOWN' AS internal_id,
    false AS deleted,
    -1 AS store_number,
    -1 AS sequence_id,
    '!UNKNOWN' AS _source_label,
    '!UNKNOWN' AS _institution_ror,
    '1900-01-01'::timestamp AS _extract_datetime,
    '1900-01-01'::timestamp AS _load_datetime
)

SELECT * FROM renamed
UNION ALL
SELECT * FROM ghost_record
