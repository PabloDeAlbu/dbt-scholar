WITH source AS (
  SELECT * FROM {{ source('dspacedb5', 'bundle2bitstream') }}
),
scope AS (
  SELECT source_label, institution_ror, base_url, extract_datetime, load_datetime
  FROM {{ ref('ldg_dspacedb5__scope') }}
),
renamed AS (
  SELECT
    id::bigint AS id,
    bundle_id::integer AS bundle_id,
    bitstream_id::integer AS bitstream_id,
    bitstream_order::integer AS bitstream_order
  FROM source
),
ghost_record AS (
  SELECT
    -1::bigint AS id,
    -1::integer AS bundle_id,
    -1::integer AS bitstream_id,
    -1::integer AS bitstream_order
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
    CASE WHEN base.id = -1 THEN '!UNKNOWN' ELSE scope.source_label END AS source_label,
    CASE WHEN base.id = -1 THEN '!UNKNOWN' ELSE scope.institution_ror END AS institution_ror,
    CASE WHEN base.id = -1 THEN '!UNKNOWN' ELSE scope.base_url END AS base_url,
    CASE WHEN base.id = -1 THEN '1900-01-01'::timestamp ELSE scope.extract_datetime END AS extract_datetime,
    CASE WHEN base.id = -1 THEN '1900-01-01'::timestamp ELSE scope.load_datetime END AS load_datetime
  FROM base
  CROSS JOIN scope
)

SELECT * FROM final
