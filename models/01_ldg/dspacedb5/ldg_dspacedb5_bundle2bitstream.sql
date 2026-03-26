WITH source AS (
  SELECT * FROM {{ source('dspacedb5', 'bundle2bitstream') }}
),
context AS (
  SELECT * FROM {{ ref('ldg_dspacedb5_context') }}
),
renamed AS (
  SELECT
    id,
    bundle_id,
    bitstream_id,
    bitstream_order,
    context.source_label AS _source_label,
    context.institution_ror AS _institution_ror,
    context.extract_datetime AS _extract_datetime,
    context.load_datetime AS _load_datetime
  FROM source
  CROSS JOIN context
),
ghost_record AS (
  SELECT
    -1 AS id,
    -1 AS bundle_id,
    -1 AS bitstream_id,
    -1 AS bitstream_order,
    '!UNKNOWN' AS _source_label,
    '!UNKNOWN' AS _institution_ror,
    '1900-01-01'::timestamp AS _extract_datetime,
    '1900-01-01'::timestamp AS _load_datetime
)

SELECT * FROM renamed
UNION ALL
SELECT * FROM ghost_record
