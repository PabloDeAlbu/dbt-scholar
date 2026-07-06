WITH source AS (
  SELECT * FROM {{ source('dspacedb5', 'metadataschemaregistry') }}
),
scope AS (
  SELECT source_label, institution_ror, base_url, extract_datetime, load_datetime
  FROM {{ ref('ldg_dspacedb5__scope') }}
),
renamed AS (
  SELECT
    metadata_schema_id::bigint AS metadata_schema_id,
    namespace::text AS namespace,
    short_id::text AS short_id
  FROM source
),
ghost_record AS (
  SELECT
    -1::bigint AS metadata_schema_id,
    '!UNKNOWN'::text AS namespace,
    '!UNKNOWN'::text AS short_id
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
