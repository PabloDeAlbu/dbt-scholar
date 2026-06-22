WITH source AS (
  SELECT * FROM {{ source('dspacedb5', 'bitstream') }}
),
context AS (
  SELECT * FROM {{ ref('ldg_dspacedb5__context') }}
),
renamed AS (
  SELECT
    bitstream_id::bigint AS bitstream_id,
    bitstream_format_id::bigint AS bitstream_format_id,
    size_bytes::bigint AS size_bytes,
    checksum::text AS checksum,
    checksum_algorithm::text AS checksum_algorithm,
    internal_id::text AS internal_id,
    deleted::boolean AS deleted,
    store_number::integer AS store_number,
    sequence_id::bigint AS sequence_id,
    context.source_label::text AS _source_label,
    context.institution_ror::text AS _institution_ror,
    context.extract_datetime::timestamp AS _extract_datetime,
    context.load_datetime::timestamp AS _load_datetime
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
