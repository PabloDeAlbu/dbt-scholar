WITH source AS (
  SELECT * FROM {{ source('dspacedb5', 'bitstream') }}
),
scope AS (
  SELECT source_label, institution_ror, base_url, extract_datetime, load_datetime
  FROM {{ ref('ldg_dspacedb5__scope') }}
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
    sequence_id::bigint AS sequence_id
  FROM source
),
ghost_record AS (
  SELECT
    -1::bigint AS bitstream_id,
    -1::bigint AS bitstream_format_id,
    -1::bigint AS size_bytes,
    '!UNKNOWN'::text AS checksum,
    '!UNKNOWN'::text AS checksum_algorithm,
    '!UNKNOWN'::text AS internal_id,
    false::boolean AS deleted,
    -1::integer AS store_number,
    -1::bigint AS sequence_id
),
base AS (
  SELECT
    renamed.*
  FROM renamed
  UNION ALL
  SELECT
    ghost_record.*
  FROM ghost_record
),
final AS (
  SELECT
    base.*,
    scope.source_label,
    scope.institution_ror,
    scope.base_url,
    scope.extract_datetime,
    scope.load_datetime
  FROM base
  CROSS JOIN scope
)

SELECT * FROM final
