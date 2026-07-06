WITH source AS (
  SELECT * FROM {{ source('dspacedb5', 'item2bundle') }}
),
scope AS (
  SELECT source_label, institution_ror, base_url, extract_datetime, load_datetime
  FROM {{ ref('ldg_dspacedb5__scope') }}
),
renamed AS (
  SELECT
    id::bigint AS id,
    item_id::integer AS item_id,
    bundle_id::integer AS bundle_id
  FROM source
),
ghost_record AS (
  SELECT
    -1::bigint AS id,
    -1::integer AS item_id,
    -1::integer AS bundle_id
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
