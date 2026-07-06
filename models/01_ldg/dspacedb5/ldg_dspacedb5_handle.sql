WITH source AS (
  SELECT * FROM {{ source('dspacedb5', 'handle') }}
),
scope AS (
  SELECT source_label, institution_ror, base_url, extract_datetime, load_datetime
  FROM {{ ref('ldg_dspacedb5__scope') }}
),
renamed AS (
  SELECT
    handle_id::bigint AS handle_id,
    handle::text AS handle,
    resource_type_id::integer AS resource_type_id,
    resource_id::integer AS resource_id
  FROM source
),
ghost_record AS (
  SELECT
    -1::bigint AS handle_id,
    '!UNKNOWN'::text AS handle,
    -1::integer AS resource_type_id,
    -1::integer AS resource_id
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
