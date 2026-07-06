WITH source AS (
  SELECT * FROM {{ source('dspacedb5', 'community2collection') }}
),
scope AS (
  SELECT source_label, institution_ror, base_url, extract_datetime, load_datetime
  FROM {{ ref('ldg_dspacedb5__scope') }}
),
renamed AS (
  SELECT
    id::bigint AS community_collection_id,
    community_id::integer AS community_id,
    collection_id::integer AS collection_id
  FROM source
),
ghost_record AS (
  SELECT
    -1::bigint AS community_collection_id,
    -1::integer AS community_id,
    -1::integer AS collection_id
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
