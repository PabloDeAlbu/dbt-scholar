WITH source AS (
  SELECT * FROM {{ source('dspacedb5', 'metadatafieldregistry') }}
),
scope AS (
  SELECT source_label, institution_ror, base_url, extract_datetime, load_datetime
  FROM {{ ref('ldg_dspacedb5__scope') }}
),
renamed AS (
  SELECT
    metadata_field_id::bigint AS metadata_field_id,
    metadata_schema_id::bigint AS metadata_schema_id,
    element::text AS element,
    qualifier::text AS qualifier,
    scope_note::text AS scope_note
  FROM source
),
ghost_record AS (
  SELECT
    -1::bigint AS metadata_field_id,
    -1::bigint AS metadata_schema_id,
    '!UNKNOWN'::text AS element,
    '!UNKNOWN'::text AS qualifier,
    '!UNKNOWN'::text AS scope_note
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
